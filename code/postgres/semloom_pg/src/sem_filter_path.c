/* Planner carrier for an exact unary SemFilter reference path. */
#include "postgres.h"

#include <math.h>

#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"

#include "semantic_filter_contract.h"
#include "sem_filter_calibration.h"
#include "sem_filter_cost.h"
#include "sem_path_common.h"
#include "sem_plan_spec.h"
#include "semloom_pg.h"

static FuncExpr *semloom_supported_filter_marker(RelOptInfo *rel,
										  Oid recording_oid,
										  Oid exact_oid);
static void semloom_validate_filter_query_shape(PlannerInfo *root,
									 Oid recording_oid,
									 Oid exact_oid);
static CustomPath *semloom_make_filter_path(PlannerInfo *root,
										 RelOptInfo *rel,
										 Index rti,
										 RangeTblEntry *rte,
										 Path *child_path,
										 FuncExpr *marker);
static Plan *semloom_plan_filter_path(PlannerInfo *root,
									 RelOptInfo *rel,
									 CustomPath *best_path,
									 List *target_list,
									 List *clauses,
									 List *custom_plans);
static Node *semloom_replace_filter_marker(Node *node, void *context);
static void semloom_replace_filter_marker_in_plan(Plan *plan,
												 Oid recording_oid,
												 Oid exact_oid);
static int semloom_filter_marker_count(Node *node,
									  Oid recording_oid,
									  Oid exact_oid);
static bool semloom_is_filter_marker(Oid function_oid,
									Oid recording_oid,
									Oid exact_oid);
static void semloom_exact_filter_arguments(FuncExpr *marker,
										 char **instruction,
										 char **model_id,
										 bool *choice_profile);
static bool semloom_json_key_equals(const JsonbValue *key, const char *expected);
static bool semloom_numeric_text_is_zero(const char *value);
static void semloom_estimate_exact_filter_cost(
	PlannerInfo *root,
	RelOptInfo *rel,
	Index rti,
	RangeTblEntry *rte,
	FuncExpr *marker,
	Node *input,
	const char *instruction,
	SemloomFilterCostEstimate *estimate);
static int32 semloom_filter_input_width(
	Index rti,
	RangeTblEntry *rte,
	Node *input);
pg_noreturn static void semloom_invalid_exact_filter_argument(const char *message);

/* Fallback heuristic used only when no matched reference artifact is accepted. */
#define SEMLOOM_FILTER_ESTIMATED_BYTES_PER_TOKEN 4.0
#define SEMLOOM_FILTER_CHAT_TEMPLATE_TOKENS 8.0

typedef struct SemloomFilterMarkerContext
{
	Oid recording_oid;
	Oid exact_oid;
} SemloomFilterMarkerContext;

static const CustomPathMethods semloom_filter_path_methods = {
	.CustomName = SEMLOOM_FILTER_CUSTOM_SCAN_NAME,
	.PlanCustomPath = semloom_plan_filter_path,
};

void
semloom_add_sem_filter_paths(PlannerInfo *root,
							 RelOptInfo *rel,
							 Index rti,
							 RangeTblEntry *rte)
{
	Oid recording_oid = semloom_filter_function_oid();
	Oid exact_oid = semloom_exact_filter_function_oid();
	FuncExpr *marker;
	List *semantic_paths = NIL;
	ListCell *cell;

	if ((!OidIsValid(recording_oid) && !OidIsValid(exact_oid)) ||
		root->parse->jointree == NULL ||
		semloom_filter_marker_count(root->parse->jointree->quals,
									  recording_oid,
									  exact_oid) == 0)
		return;
	semloom_validate_filter_query_shape(root, recording_oid, exact_oid);
	if (root->parse->jointree == NULL ||
		list_length(root->parse->jointree->fromlist) != 1 ||
		!IsA(linitial(root->parse->jointree->fromlist), RangeTblRef) ||
		linitial_node(RangeTblRef, root->parse->jointree->fromlist)->rtindex != rti)
		return;

	marker = semloom_supported_filter_marker(rel, recording_oid, exact_oid);
	foreach(cell, rel->pathlist)
	{
		Path *child_path = lfirst_node(Path, cell);

		semantic_paths = lappend(semantic_paths,
								 semloom_make_filter_path(root,
												  rel,
												  rti,
												  rte,
												  child_path,
												  marker));
	}
	if (semantic_paths == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemFilter lowering found no ordinary child path")));

	rel->pathlist = semantic_paths;
	rel->partial_pathlist = NIL;
}

static FuncExpr *
semloom_supported_filter_marker(RelOptInfo *rel,
								Oid recording_oid,
								Oid exact_oid)
{
	FuncExpr *marker = NULL;
	ListCell *cell;

	foreach(cell, rel->baserestrictinfo)
	{
		RestrictInfo *restriction = lfirst_node(RestrictInfo, cell);
		int count = semloom_filter_marker_count((Node *) restriction->clause,
										 recording_oid,
										 exact_oid);

		if (count == 0)
			continue;
		if (count != 1 || !IsA(restriction->clause, FuncExpr) ||
			!semloom_is_filter_marker(((FuncExpr *) restriction->clause)->funcid,
									 recording_oid,
									 exact_oid) ||
			marker != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ai_semantic.filter must be one top-level AND predicate")));
		marker = (FuncExpr *) restriction->clause;
	}
	if (marker == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ai_semantic.filter must be a base-relation predicate")));
	if (marker->funcid == recording_oid &&
		(list_length(marker->args) != 1 ||
		 exprType(linitial(marker->args)) != TEXTOID ||
		 marker->funcresulttype != BOOLOID))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("ai_semantic.filter capability requires one text input and boolean output")));
	if (marker->funcid == exact_oid &&
		(list_length(marker->args) != 3 ||
		 exprType(linitial(marker->args)) != TEXTOID ||
		 exprType(lsecond(marker->args)) != TEXTOID ||
		 exprType(lthird(marker->args)) != JSONBOID ||
		 marker->funcresulttype != BOOLOID))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("exact ai_semantic.filter requires text, text, jsonb and boolean output")));
	return marker;
}

static void
semloom_validate_filter_query_shape(PlannerInfo *root,
									Oid recording_oid,
									Oid exact_oid)
{
	Query *parse = root->parse;
	bool insert_source = semloom_is_insert_source(root);
	Oid map_oid = semloom_map_function_oid();
	RangeTblRef *range_reference;
	RangeTblEntry *range_entry;

	if ((root->query_level != 1 && !insert_source) || parse->commandType != CMD_SELECT ||
		parse->setOperations != NULL || parse->cteList != NIL || parse->hasAggs ||
		parse->groupClause != NIL || parse->groupingSets != NIL ||
		parse->havingQual != NULL || parse->hasWindowFuncs ||
		parse->windowClause != NIL || parse->distinctClause != NIL ||
		parse->rowMarks != NIL || parse->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("query shape is outside the current SemFilter capability"),
				 errdetail("Only a single-table SELECT or INSERT ... SELECT with ordinary filters, projections, ORDER BY, and LIMIT is supported.")));
	if (insert_source &&
		(root->parent_root->parse->resultRelation == 0 ||
		 root->parent_root->parse->onConflict != NULL ||
		 root->parent_root->parse->returningList != NIL ||
		 root->parent_root->parse->override != OVERRIDING_NOT_SET))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT shape is outside the current SemFilter capability"),
				 errdetail("ON CONFLICT, RETURNING, and OVERRIDING are not supported.")));
	if (parse->jointree == NULL || list_length(parse->jointree->fromlist) != 1 ||
		!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemFilter capability requires one non-inherited table")));
	range_reference = linitial_node(RangeTblRef, parse->jointree->fromlist);
	range_entry = rt_fetch(range_reference->rtindex, parse->rtable);
	if (range_entry->rtekind != RTE_RELATION || range_entry->inh ||
		range_entry->tablesample != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemFilter capability requires one non-inherited table")));
	if (semloom_filter_marker_count((Node *) parse->targetList,
									 recording_oid, exact_oid) != 0 ||
		semloom_filter_marker_count(parse->limitOffset,
									 recording_oid, exact_oid) != 0 ||
		semloom_filter_marker_count(parse->limitCount,
									 recording_oid, exact_oid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ai_semantic.filter is only supported as a WHERE predicate")));
	if (OidIsValid(map_oid) &&
		semloom_marker_count((Node *) parse->targetList, map_oid) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("SemMap and SemFilter cannot be combined in the current capability")));
}

static CustomPath *
semloom_make_filter_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Index rti,
						 RangeTblEntry *rte,
						 Path *child_path,
						 FuncExpr *marker)
{
	CustomPath *path = makeNode(CustomPath);
	PathTarget *child_target;
	Path *projected_child = child_path;
	Node *input = linitial(marker->args);
	List *plan_private;
	double path_rows = child_path->rows;
	double ai_work_cost = cpu_operator_cost * child_path->rows;
	int input_column = 0;
	int column = 1;
	ListCell *cell;

	if (child_path->param_info != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parameterized SemFilter paths are not supported")));
	child_target = copy_pathtarget(child_path->pathtarget);
	foreach(cell, child_target->exprs)
	{
		if (equal(lfirst(cell), input))
		{
			input_column = column;
			break;
		}
		column++;
	}
	if (input_column == 0)
	{
		add_column_to_pathtarget(child_target,
								 (Expr *) copyObject(input),
								 0);
		set_pathtarget_cost_width(root, child_target);
		input_column = list_length(child_target->exprs);
		projected_child = (Path *) create_projection_path(root,
												   rel,
												   child_path,
												   child_target);
	}
	if (marker->funcid == semloom_exact_filter_function_oid())
	{
		char *instruction = NULL;
		char *model_id = NULL;
		bool choice_profile = false;

		semloom_exact_filter_arguments(marker, &instruction, &model_id, &choice_profile);
		plan_private = choice_profile ?
			semloom_plan_spec_make_choice_filter_private(instruction, model_id, (AttrNumber) input_column) :
			semloom_plan_spec_make_exact_filter_private(instruction, model_id, (AttrNumber) input_column);
		{
			AttrNumber calibration_input_column;
			SemloomFilterCalibration calibration;
			SemloomFilterCostEstimate estimate;
			SemloomPlanSpec plan_spec;

			semloom_estimate_exact_filter_cost(root,
										   rel,
										   rti,
										   rte,
										   marker,
										   input,
										   instruction,
										   &estimate);
			semloom_plan_spec_decode(plan_private,
								 CurrentMemoryContext,
								 &plan_spec,
								 &calibration_input_column);
			Assert(calibration_input_column == (AttrNumber) input_column);
			semloom_filter_calibration_load(
				&plan_spec,
				semloom_provider_execution_profile_name(),
				&calibration);
			semloom_filter_calibration_apply(&calibration, &estimate);
			plan_private = lappend(
				plan_private,
				semloom_filter_cost_make_private(&estimate));
			path_rows = clamp_row_est(
				estimate.semantic_input_rows * estimate.output_selectivity);
			ai_work_cost = estimate.ai_work_cost;
		}
	}
	else
		plan_private = semloom_plan_spec_make_recording_private(
			SEMLOOM_PLAN_OPERATOR_FILTER,
			(AttrNumber) input_column);

	path->path.pathtype = T_CustomScan;
	path->path.parent = rel;
	path->path.pathtarget = rel->reltarget;
	path->path.param_info = NULL;
	path->path.parallel_aware = false;
	path->path.parallel_safe = false;
	path->path.parallel_workers = 0;
	path->path.rows = path_rows;
	path->path.disabled_nodes = child_path->disabled_nodes;
	path->path.startup_cost = child_path->startup_cost;
	path->path.total_cost = child_path->total_cost + ai_work_cost;
	path->path.pathkeys = child_path->pathkeys;
	path->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	path->custom_paths = list_make1(projected_child);
	path->custom_restrictinfo = NIL;
	path->custom_private = plan_private;
	path->methods = &semloom_filter_path_methods;
	return path;
}

static Plan *
semloom_plan_filter_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 CustomPath *best_path,
						 List *target_list,
						 List *clauses,
						 List *custom_plans)
{
	Oid recording_oid = semloom_filter_function_oid();
	Oid exact_oid = semloom_exact_filter_function_oid();
	CustomScan *scan = makeNode(CustomScan);
	Plan *child_plan;
	SemloomFilterCostEstimate cost_estimate;
	bool has_cost_estimate;
	int input_column;

	(void) root;
	(void) rel;
	(void) clauses;
	if ((!OidIsValid(recording_oid) && !OidIsValid(exact_oid)) ||
		list_length(custom_plans) != 1 ||
		(list_length(best_path->custom_private) != 2 &&
		 list_length(best_path->custom_private) != 3) ||
		!IsA(lsecond(best_path->custom_private), Integer))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid SemFilter custom path state")));
	child_plan = linitial_node(Plan, custom_plans);
	input_column = intVal(lsecond(best_path->custom_private));
	if (input_column <= 0 || input_column > list_length(child_plan->targetlist))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemFilter plan lost its input identity")));

	semloom_replace_filter_marker_in_plan(child_plan, recording_oid, exact_oid);
	has_cost_estimate = semloom_filter_cost_decode(
		best_path->custom_private,
		&cost_estimate);
	if (has_cost_estimate)
		child_plan->plan_rows = cost_estimate.semantic_input_rows;
	scan->scan.plan.targetlist = copyObject(target_list);
	scan->scan.plan.qual = NIL;
	scan->scan.scanrelid = 0;
	scan->flags = CUSTOMPATH_SUPPORT_PROJECTION;
	scan->custom_plans = custom_plans;
	scan->custom_exprs = NIL;
	scan->custom_private = copyObject(best_path->custom_private);
	scan->custom_scan_tlist = copyObject(child_plan->targetlist);
	scan->methods = &semloom_filter_scan_methods;
	return &scan->scan.plan;
}

static void
semloom_estimate_exact_filter_cost(
	PlannerInfo *root,
	RelOptInfo *rel,
	Index rti,
	RangeTblEntry *rte,
	FuncExpr *marker,
	Node *input,
	const char *instruction,
	SemloomFilterCostEstimate *estimate)
{
	List *ordinary_restrictions = NIL;
	ListCell *cell;
	NullTest *null_test = makeNode(NullTest);
	Selectivity ordinary_selectivity;
	Selectivity null_selectivity;
	Selectivity semantic_selectivity;
	double nonnull_selectivity;
	double prompt_tokens_per_call;
	double prompt_content_bytes;
	int32 input_width;

	foreach(cell, rel->baserestrictinfo)
	{
		RestrictInfo *restriction = lfirst_node(RestrictInfo, cell);

		if (!equal(restriction->clause, marker))
			ordinary_restrictions = lappend(ordinary_restrictions, restriction);
	}
	ordinary_selectivity = clauselist_selectivity(root,
												ordinary_restrictions,
												rel->relid,
												JOIN_INNER,
												NULL);
	null_test->arg = (Expr *) copyObject(input);
	null_test->nulltesttype = IS_NULL;
	null_test->argisrow = false;
	null_test->location = -1;
	null_selectivity = clause_selectivity(root,
									(Node *) null_test,
									rel->relid,
									JOIN_INNER,
									NULL);
	semantic_selectivity = clause_selectivity(root,
											 (Node *) marker,
											 rel->relid,
											 JOIN_INNER,
											 NULL);
	nonnull_selectivity = 1.0 - null_selectivity;
	input_width = semloom_filter_input_width(rti, rte, input);
	prompt_content_bytes = strlen(SEMLOOM_FILTER_SYSTEM_DIRECTIVE) +
		strlen(SEMLOOM_FILTER_INSTRUCTION_SEPARATOR) +
		strlen(instruction) + input_width;
	prompt_tokens_per_call =
		ceil(prompt_content_bytes / SEMLOOM_FILTER_ESTIMATED_BYTES_PER_TOKEN) +
		SEMLOOM_FILTER_CHAT_TEMPLATE_TOKENS;

	MemSet(estimate, 0, sizeof(*estimate));
	estimate->cost_model_id = SEMLOOM_FILTER_COST_MODEL_ID;
	estimate->calibration_status = SEMLOOM_FILTER_COST_CALIBRATION_STATUS;
	estimate->calibration_reason = "not-configured";
	estimate->calibration_id = "";
	estimate->workload_signature = "";
	estimate->service_signature = "";
	estimate->model_role = SEMLOOM_EXACT_FILTER_ROLE;
	estimate->semantic_input_rows = clamp_row_est(
		rel->tuples * ordinary_selectivity);
	estimate->output_selectivity =
		Max(0.0, Min(1.0, nonnull_selectivity * semantic_selectivity));
	estimate->estimated_model_calls =
		estimate->semantic_input_rows * nonnull_selectivity;
	estimate->estimated_prompt_tokens =
		estimate->estimated_model_calls * prompt_tokens_per_call;
	estimate->estimated_output_tokens =
		estimate->estimated_model_calls * SEMLOOM_FILTER_MAX_TOKENS;
	estimate->ai_work_cost = cpu_operator_cost *
		(estimate->estimated_model_calls +
		 estimate->estimated_prompt_tokens +
		 estimate->estimated_output_tokens);
}

static int32
semloom_filter_input_width(Index rti, RangeTblEntry *rte, Node *input)
{
	Node *stripped_input = strip_implicit_coercions(input);
	int32 width = 0;

	if (IsA(stripped_input, Var))
	{
		Var *variable = (Var *) stripped_input;

		if (variable->varno == rti && variable->varattno > 0)
			width = get_attavgwidth(rte->relid, variable->varattno);
	}
	if (width <= 0)
		width = get_typavgwidth(exprType(input), exprTypmod(input));
	return Max(width, 1);
}

static Node *
semloom_replace_filter_marker(Node *node, void *context)
{
	SemloomFilterMarkerContext *marker_context = context;

	if (node == NULL)
		return NULL;
	if (IsA(node, FuncExpr) &&
		semloom_is_filter_marker(((FuncExpr *) node)->funcid,
									 marker_context->recording_oid,
									 marker_context->exact_oid))
		return (Node *) makeBoolConst(true, false);
	return expression_tree_mutator(node, semloom_replace_filter_marker, context);
}

static void
semloom_replace_filter_marker_in_plan(Plan *plan,
									 Oid recording_oid,
									 Oid exact_oid)
{
	SemloomFilterMarkerContext context = {
		.recording_oid = recording_oid,
		.exact_oid = exact_oid,
	};

	if (plan == NULL)
		return;
	plan->targetlist = (List *) semloom_replace_filter_marker(
		(Node *) plan->targetlist,
		&context);
	plan->qual = (List *) semloom_replace_filter_marker((Node *) plan->qual,
												  &context);
	semloom_replace_filter_marker_in_plan(plan->lefttree, recording_oid, exact_oid);
	semloom_replace_filter_marker_in_plan(plan->righttree, recording_oid, exact_oid);
}

static int
semloom_filter_marker_count(Node *node, Oid recording_oid, Oid exact_oid)
{
	int count = 0;

	if (OidIsValid(recording_oid))
		count += semloom_marker_count(node, recording_oid);
	if (OidIsValid(exact_oid) && exact_oid != recording_oid)
		count += semloom_marker_count(node, exact_oid);
	return count;
}

static bool
semloom_is_filter_marker(Oid function_oid, Oid recording_oid, Oid exact_oid)
{
	return (OidIsValid(recording_oid) && function_oid == recording_oid) ||
		(OidIsValid(exact_oid) && function_oid == exact_oid);
}

static void
semloom_exact_filter_arguments(FuncExpr *marker,
								 char **instruction,
								 char **model_id,
								 bool *choice_profile)
{
	Node *instruction_node = lsecond(marker->args);
	Node *options_node = lthird(marker->args);
	Const *instruction_const;
	Const *options_const;
	text *instruction_text;
	Size instruction_length;
	Jsonb *options;
	JsonbIterator *iterator;
	JsonbValue value;
	JsonbIteratorToken token;
	bool seen_model = false;
	bool seen_temperature = false;
	bool seen_max_tokens = false;
	JsonbValue profile_key;
	JsonbValue *profile_value;

	*choice_profile = false;

	if (!IsA(instruction_node, Const) || ((Const *) instruction_node)->constisnull)
		semloom_invalid_exact_filter_argument(
			"SemFilter instruction must be a non-NULL plan-time constant");
	if (!IsA(options_node, Const) || ((Const *) options_node)->constisnull)
		semloom_invalid_exact_filter_argument(
			"SemFilter options must be a non-NULL plan-time constant");
	instruction_const = (Const *) instruction_node;
	options_const = (Const *) options_node;
	if (instruction_const->consttype != TEXTOID || options_const->consttype != JSONBOID)
		semloom_invalid_exact_filter_argument(
			"SemFilter instruction and options have invalid types");

	instruction_text = DatumGetTextPP(instruction_const->constvalue);
	instruction_length = VARSIZE_ANY_EXHDR(instruction_text);
	if (instruction_length == 0 ||
		instruction_length > SEMLOOM_FILTER_INSTRUCTION_MAX_BYTES ||
		memchr(VARDATA_ANY(instruction_text), '\0', instruction_length) != NULL)
		semloom_invalid_exact_filter_argument(
			"SemFilter instruction must contain 1 to 4096 UTF8 bytes");
	*instruction = pnstrdup(VARDATA_ANY(instruction_text), instruction_length);

	options = DatumGetJsonbP(options_const->constvalue);
	profile_key.type = jbvString;
	profile_key.val.string.val = "generation_profile";
	profile_key.val.string.len = strlen(profile_key.val.string.val);
	profile_value = JB_ROOT_IS_OBJECT(options) ?
		findJsonbValueFromContainer(&options->root, JB_FOBJECT, &profile_key) : NULL;
	/* No selector: retain the old count/error boundary, including extra keys. */
	if (!JB_ROOT_IS_OBJECT(options) ||
		JB_ROOT_COUNT(options) != (profile_value == NULL ? 3 : 4))
		semloom_invalid_exact_filter_argument(
			"SemFilter options must contain exactly model, temperature, and max_tokens");
	if (profile_value != NULL)
	{
		if (!semloom_json_key_equals(profile_value, SEMLOOM_CHOICE_FILTER_PROFILE_SELECTOR))
			semloom_invalid_exact_filter_argument("unsupported SemFilter generation_profile");
		*choice_profile = true;
	}
	iterator = JsonbIteratorInit(&options->root);
	while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
	{
		JsonbValue option_value;

		if (token != WJB_KEY)
			continue;
		if (JsonbIteratorNext(&iterator, &option_value, true) != WJB_VALUE)
			semloom_invalid_exact_filter_argument("invalid SemFilter option value");
		if (semloom_json_key_equals(&value, "model"))
		{
			if (seen_model || option_value.type != jbvString ||
				option_value.val.string.len <= 0 ||
				option_value.val.string.len > SEMLOOM_FILTER_MODEL_MAX_BYTES ||
				memchr(option_value.val.string.val,
					   '\0',
					   option_value.val.string.len) != NULL)
				semloom_invalid_exact_filter_argument(
					"SemFilter model must contain 1 to 128 UTF8 bytes");
			*model_id = pnstrdup(option_value.val.string.val,
								 option_value.val.string.len);
			seen_model = true;
		}
		else if (semloom_json_key_equals(&value, "temperature"))
		{
			char *numeric_text;

			if (seen_temperature || option_value.type != jbvNumeric)
				semloom_invalid_exact_filter_argument(
					"SemFilter temperature must be numeric zero");
			numeric_text = DatumGetCString(DirectFunctionCall1(
				numeric_out,
				NumericGetDatum(option_value.val.numeric)));
			if (!semloom_numeric_text_is_zero(numeric_text))
				semloom_invalid_exact_filter_argument(
					"SemFilter temperature must be numeric zero");
			pfree(numeric_text);
			seen_temperature = true;
		}
		else if (semloom_json_key_equals(&value, "max_tokens"))
		{
			char *numeric_text;

			if (seen_max_tokens || option_value.type != jbvNumeric)
				semloom_invalid_exact_filter_argument(
					"SemFilter max_tokens must be integer 8");
			numeric_text = DatumGetCString(DirectFunctionCall1(
				numeric_out,
				NumericGetDatum(option_value.val.numeric)));
			if (strcmp(numeric_text, "8") != 0)
				semloom_invalid_exact_filter_argument(
					"SemFilter max_tokens must be integer 8");
			pfree(numeric_text);
			seen_max_tokens = true;
		}
		else if (semloom_json_key_equals(&value, "generation_profile"))
		{
			/* Its type and exact supported selector were checked above. */
			Assert(*choice_profile);
		}
		else
			semloom_invalid_exact_filter_argument(
				"SemFilter options must contain exactly model, temperature, and max_tokens");
	}
	if (!seen_model || !seen_temperature || !seen_max_tokens)
		semloom_invalid_exact_filter_argument(
			"SemFilter options must contain exactly model, temperature, and max_tokens");
}

static bool
semloom_json_key_equals(const JsonbValue *key, const char *expected)
{
	Size expected_length = strlen(expected);

	return key->type == jbvString &&
		key->val.string.len == expected_length &&
		memcmp(key->val.string.val, expected, expected_length) == 0;
}

static bool
semloom_numeric_text_is_zero(const char *value)
{
	const char *cursor = value;
	bool saw_zero = false;

	if (*cursor == '-')
		cursor++;
	for (; *cursor != '\0'; cursor++)
	{
		if (*cursor == '0')
			saw_zero = true;
		else if (*cursor != '.')
			return false;
	}
	return saw_zero;
}

static void
semloom_invalid_exact_filter_argument(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("%s", message)));
	pg_unreachable();
}
