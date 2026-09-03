#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"

#include "semantic_map_contract.h"
#include "sem_text.h"
#include "sem_path_common.h"
#include "sem_plan_spec.h"
#include "semloom_pg.h"

static FuncExpr *semloom_supported_marker(Query *parse, Oid marker_oid);
static Oid semloom_map_marker_oid(Query *parse);
static List *semloom_generate_map_private(FuncExpr *marker, AttrNumber input_column);
static void semloom_validate_query_shape(PlannerInfo *root, Oid marker_oid);
static CustomPath *semloom_make_path(RelOptInfo *parent_rel, Path *child_path);
static Plan *semloom_plan_path(PlannerInfo *root,
								RelOptInfo *rel,
								CustomPath *best_path,
								List *target_list,
								List *clauses,
								List *custom_plans);
static Node *semloom_replace_marker(Node *node, void *context);
static void semloom_replace_marker_in_plan(Plan *plan, Oid marker_oid);

static const CustomPathMethods semloom_path_methods = {
	.CustomName = SEMLOOM_MAP_CUSTOM_SCAN_NAME,
	.PlanCustomPath = semloom_plan_path,
};

static Oid
semloom_map_marker_oid(Query *parse)
{
	Oid recording_oid = semloom_map_function_oid();
	Oid generate_oid = semloom_generate_map_function_oid();
	int recording_count = OidIsValid(recording_oid) ?
		semloom_marker_count((Node *) parse->targetList, recording_oid) : 0;
	int generate_count = OidIsValid(generate_oid) ?
		semloom_marker_count((Node *) parse->targetList, generate_oid) : 0;

	if (recording_count > 0 && generate_count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemMap capability supports exactly one visible marker")));
	return generate_count > 0 ? generate_oid : recording_oid;
}

static FuncExpr *
semloom_supported_marker(Query *parse, Oid marker_oid)
{
	FuncExpr *supported = NULL;
	ListCell *cell;

	foreach(cell, parse->targetList)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);
		int count = semloom_marker_count((Node *) entry->expr, marker_oid);

		if (count == 0)
			continue;
		if (count != 1 || !IsA(entry->expr, FuncExpr) ||
			((FuncExpr *) entry->expr)->funcid != marker_oid)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("nested ai_semantic.map expressions are not supported")));
		if (supported != NULL || entry->resjunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("the SemMap capability supports exactly one visible marker")));

		supported = (FuncExpr *) entry->expr;
	}

	return supported;
}

static void
semloom_validate_query_shape(PlannerInfo *root, Oid marker_oid)
{
	Query *parse = root->parse;
	FuncExpr *marker = semloom_supported_marker(parse, marker_oid);
	bool insert_source = semloom_is_insert_source(root);
	RangeTblRef *range_reference;
	RangeTblEntry *range_entry;

	if (marker == NULL)
		return;
	if ((root->query_level != 1 && !insert_source) || parse->commandType != CMD_SELECT ||
		parse->setOperations != NULL || parse->cteList != NIL || parse->hasAggs ||
		parse->groupClause != NIL || parse->groupingSets != NIL || parse->havingQual != NULL ||
		parse->hasWindowFuncs || parse->windowClause != NIL || parse->distinctClause != NIL ||
		parse->sortClause != NIL || parse->rowMarks != NIL || parse->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("query shape is outside the current SemMap capability"),
				 errdetail("Only a single-table SELECT or INSERT ... SELECT with ordinary filters, projections, and LIMIT is supported.")));
	if (insert_source &&
		(root->parent_root->parse->resultRelation == 0 ||
		 root->parent_root->parse->onConflict != NULL ||
		 root->parent_root->parse->returningList != NIL ||
		 root->parent_root->parse->override != OVERRIDING_NOT_SET))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT shape is outside the current SemMap capability"),
				 errdetail("ON CONFLICT, RETURNING, and OVERRIDING are not supported.")));
	if (parse->jointree == NULL || list_length(parse->jointree->fromlist) != 1 ||
		!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemMap capability requires exactly one base relation")));

	range_reference = linitial_node(RangeTblRef, parse->jointree->fromlist);
	range_entry = rt_fetch(range_reference->rtindex, parse->rtable);
	if (range_entry->rtekind != RTE_RELATION || range_entry->inh || range_entry->tablesample != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemMap capability requires one non-inherited table")));
	if (semloom_marker_count(parse->jointree->quals, marker_oid) != 0 ||
		semloom_marker_count(parse->limitOffset, marker_oid) != 0 ||
		semloom_marker_count(parse->limitCount, marker_oid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ai_semantic.map is only supported as a top-level output expression")));
	if (list_length(marker->args) != (marker_oid == semloom_generate_map_function_oid() ? 3 : 1) ||
		exprType(linitial(marker->args)) != TEXTOID ||
		marker->funcresulttype != TEXTOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("ai_semantic.map capability requires one text input and text output")));
}

void
semloom_add_sem_map_paths(PlannerInfo *root,
						   UpperRelationKind stage,
						   RelOptInfo *input_rel,
						   RelOptInfo *output_rel)
{
	Oid marker_oid;
	List *semantic_paths = NIL;
	ListCell *cell;

	if (stage != UPPERREL_FINAL)
		return;
	marker_oid = semloom_map_marker_oid(root->parse);
	if (!OidIsValid(marker_oid) ||
		semloom_marker_count((Node *) root->parse->targetList, marker_oid) == 0)
		return;

	semloom_validate_query_shape(root, marker_oid);
	foreach(cell, output_rel->pathlist)
	{
		Path *child_path = lfirst_node(Path, cell);

		semantic_paths = lappend(semantic_paths, semloom_make_path(output_rel, child_path));
	}
	if (semantic_paths == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap lowering found no ordinary child path")));

	output_rel->pathlist = semantic_paths;
	output_rel->partial_pathlist = NIL;
}

static CustomPath *
semloom_make_path(RelOptInfo *parent_rel, Path *child_path)
{
	CustomPath *path = makeNode(CustomPath);

	if (child_path->param_info != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parameterized SemMap paths are not supported")));

	path->path.pathtype = T_CustomScan;
	path->path.parent = parent_rel;
	path->path.pathtarget = child_path->pathtarget;
	path->path.param_info = NULL;
	path->path.parallel_aware = false;
	path->path.parallel_safe = false;
	path->path.parallel_workers = 0;
	path->path.rows = child_path->rows;
	path->path.disabled_nodes = child_path->disabled_nodes;
	path->path.startup_cost = child_path->startup_cost;
	path->path.total_cost = child_path->total_cost + cpu_operator_cost * child_path->rows;
	path->path.pathkeys = child_path->pathkeys;
	path->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	path->custom_paths = list_make1(child_path);
	path->custom_restrictinfo = NIL;
	path->custom_private = NIL;
	path->methods = &semloom_path_methods;

	return path;
}

static Plan *
semloom_plan_path(PlannerInfo *root,
				   RelOptInfo *rel,
				   CustomPath *best_path,
				   List *target_list,
				   List *clauses,
				   List *custom_plans)
{
	Oid marker_oid = semloom_map_marker_oid(root->parse);
	CustomScan *scan = makeNode(CustomScan);
	FuncExpr *marker = NULL;
	List *scan_target_list;
	List *mapped_columns = NIL;
	ListCell *cell;

	if (!OidIsValid(marker_oid) || list_length(custom_plans) != 1 || clauses != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid SemMap custom path state")));

	semloom_replace_marker_in_plan(linitial_node(Plan, custom_plans), marker_oid);
	scan_target_list = (List *) semloom_replace_marker((Node *) target_list, &marker_oid);
	foreach(cell, target_list)
	{
		TargetEntry *entry = lfirst_node(TargetEntry, cell);

		if (IsA(entry->expr, FuncExpr) &&
			((FuncExpr *) entry->expr)->funcid == marker_oid)
		{
			mapped_columns = lappend_int(mapped_columns, entry->resno);
			marker = (FuncExpr *) entry->expr;
		}
	}
	if (list_length(mapped_columns) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemMap plan lost its mapped output identity")));

	/*
	 * Leave these expressions in planner form.  set_customscan_references()
	 * matches them against custom_scan_tlist and creates INDEX_VAR references
	 * after the complete plan tree is available.
	 */
	scan->scan.plan.targetlist = copyObject(scan_target_list);
	scan->scan.plan.qual = NIL;
	scan->scan.scanrelid = 0;
	scan->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	scan->custom_plans = custom_plans;
	scan->custom_exprs = NIL;
	if (marker_oid == semloom_generate_map_function_oid())
	{
		scan->custom_private = semloom_generate_map_private(marker,
			(AttrNumber) linitial_int(mapped_columns));
		record_plan_function_dependency(root, marker_oid);
	}
	else
		scan->custom_private = semloom_plan_spec_make_recording_private(
			SEMLOOM_PLAN_OPERATOR_MAP, (AttrNumber) linitial_int(mapped_columns));
	scan->custom_scan_tlist = scan_target_list;
	scan->methods = &semloom_map_scan_methods;

	return &scan->scan.plan;
}

static Node *
semloom_replace_marker(Node *node, void *context)
{
	Oid marker_oid = *((Oid *) context);

	if (node == NULL)
		return NULL;
	if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == marker_oid)
	{
		FuncExpr *marker = (FuncExpr *) node;

		if (list_length(marker->args) != (marker_oid == semloom_generate_map_function_oid() ? 3 : 1))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid SemMap marker arguments during plan lowering")));
		return copyObject(linitial(marker->args));
	}

	return expression_tree_mutator(node, semloom_replace_marker, context);
}

static void
semloom_replace_marker_in_plan(Plan *plan, Oid marker_oid)
{
	if (plan == NULL)
		return;
	plan->targetlist = (List *) semloom_replace_marker((Node *) plan->targetlist, &marker_oid);
	plan->qual = (List *) semloom_replace_marker((Node *) plan->qual, &marker_oid);
	semloom_replace_marker_in_plan(plan->lefttree, marker_oid);
	semloom_replace_marker_in_plan(plan->righttree, marker_oid);
}

static JsonbValue *
semloom_map_option(Jsonb *options, const char *name)
{
	JsonbValue key;

	key.type = jbvString;
	key.val.string.val = (char *) name;
	key.val.string.len = strlen(name);
	return findJsonbValueFromContainer(&options->root, JB_FOBJECT, &key);
}

static List *
semloom_generate_map_private(FuncExpr *marker, AttrNumber input_column)
{
	Const *instruction_const;
	Const *options_const;
	text *instruction;
	Size instruction_length;
	Jsonb *options;
	JsonbValue *model;
	JsonbValue *temperature;
	JsonbValue *max_tokens;
	Datum numeric_tokens;
	int32 tokens;

	if (!IsA(lsecond(marker->args), Const) || !IsA(lthird(marker->args), Const))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("SemMap instruction and options must be plan-time constants")));
	instruction_const = lsecond_node(Const, marker->args);
	options_const = lthird_node(Const, marker->args);
	if (instruction_const->consttype != TEXTOID || options_const->consttype != JSONBOID ||
		instruction_const->constisnull || options_const->constisnull)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap instruction and options must be non-NULL text and jsonb constants")));
	instruction = DatumGetTextPP(instruction_const->constvalue);
	instruction_length = VARSIZE_ANY_EXHDR(instruction);
	if (instruction_length == 0 || instruction_length > SEMLOOM_MAP_MAX_INSTRUCTION_BYTES ||
		!semloom_text_is_utf8_no_nul((const uint8 *) VARDATA_ANY(instruction), instruction_length))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap instruction must contain 1 to 4096 UTF8 bytes")));
	options = DatumGetJsonbP(options_const->constvalue);
	if (!JB_ROOT_IS_OBJECT(options) || JB_ROOT_COUNT(options) != 3)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap options must contain exactly model, temperature, and max_tokens")));
	model = semloom_map_option(options, "model");
	temperature = semloom_map_option(options, "temperature");
	max_tokens = semloom_map_option(options, "max_tokens");
	if (model == NULL || temperature == NULL || max_tokens == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap options must contain exactly model, temperature, and max_tokens")));
	if (model->type != jbvString || model->val.string.len <= 0 ||
		model->val.string.len > SEMLOOM_MAP_MAX_MODEL_BYTES ||
		!semloom_text_is_utf8_no_nul((const uint8 *) model->val.string.val, model->val.string.len))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap model must contain 1 to 128 UTF8 bytes")));
	if (temperature->type != jbvNumeric ||
		DatumGetInt32(DirectFunctionCall2(numeric_cmp, NumericGetDatum(temperature->val.numeric),
			DirectFunctionCall1(int4_numeric, Int32GetDatum(0)))) != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap temperature must be numeric zero")));
	if (max_tokens->type != jbvNumeric)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap max_tokens must be an integer from 1 to 4096")));
	numeric_tokens = NumericGetDatum(max_tokens->val.numeric);
	if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, numeric_tokens,
			DirectFunctionCall1(int4_numeric, Int32GetDatum(1)))) < 0 ||
		DatumGetInt32(DirectFunctionCall2(numeric_cmp, numeric_tokens,
			DirectFunctionCall1(int4_numeric, Int32GetDatum(SEMLOOM_MAP_MAX_GENERATION_TOKENS)))) > 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap max_tokens must be an integer from 1 to 4096")));
	tokens = DatumGetInt32(DirectFunctionCall1(numeric_int4, numeric_tokens));
	if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, numeric_tokens,
			DirectFunctionCall1(int4_numeric, Int32GetDatum(tokens)))) != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("SemMap max_tokens must be an integer from 1 to 4096")));
	return semloom_plan_spec_make_generate_map_private(
		pnstrdup(VARDATA_ANY(instruction), instruction_length),
		pnstrdup(model->val.string.val, model->val.string.len),
		(uint32) tokens, input_column, marker->funcid);
}
