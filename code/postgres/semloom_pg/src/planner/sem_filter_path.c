/* Planner carrier for an exact unary SemFilter reference path. */
#include "postgres.h"

#include <math.h>

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "semantics/semantic_filter_contract.h"
#include "planner/sem_filter_calibration.h"
#include "planner/sem_filter_call.h"
#include "planner/sem_filter_cost.h"
#include "planner/sem_path_common.h"
#include "planner/sem_plan_spec.h"
#include "extension_config.h"
#include "planner/marker_identity.h"
#include "planner/paths.h"
#include "executor/sem_scan.h"

static void semloom_validate_filter_query_shape(PlannerInfo *root,
									 FromExpr *source,
									 Oid recording_oid,
									 Oid exact_oid);
static CustomPath *semloom_make_filter_path(PlannerInfo *root,
										 RelOptInfo *rel,
										 Index rti,
										 RangeTblEntry *rte,
										 Path *child_path,
										 SemloomFilterCall *call, bool downstream);
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
	FromExpr *source = root->parse->jointree;
	List *calls;
	List *semantic_paths = NIL;
	ListCell *cell;

	/* A pulled-up INSERT source retains its own qualification FromExpr. */
	if (root->parse->commandType == CMD_INSERT && source != NULL &&
		source->quals == NULL && list_length(source->fromlist) == 1 &&
		IsA(linitial(source->fromlist), FromExpr))
		source = linitial_node(FromExpr, source->fromlist);
	if ((!OidIsValid(recording_oid) && !OidIsValid(exact_oid)) ||
		source == NULL ||
		semloom_filter_marker_count(source->quals,
									  recording_oid,
									  exact_oid) == 0)
		return;
	semloom_validate_filter_query_shape(root, source, recording_oid, exact_oid);
	if (linitial_node(RangeTblRef, source->fromlist)->rtindex != rti)
		return;

	calls = semloom_filter_calls(root, rel, recording_oid, exact_oid);
	foreach(cell, rel->pathlist)
	{
		Path *child = lfirst_node(Path, cell);
		PathTarget *intermediate = copy_pathtarget(child->pathtarget);
		ListCell *call_cell;
		bool downstream = false;

		if (list_length(calls) > 1)
		{
			/* Pass raw dependencies; evaluate each input only at its own Filter. */
			foreach(call_cell, calls)
			{
				SemloomFilterCall *call = lfirst(call_cell);
				List *variables = pull_var_clause((Node *) semloom_call_input(call->semantic),
												PVC_RECURSE_PLACEHOLDERS);

				add_new_columns_to_pathtarget(intermediate, variables);
				list_free(variables);
			}
			set_pathtarget_cost_width(root, intermediate);
			child = (Path *) create_projection_path(root, rel, child, intermediate);
		}
		foreach(call_cell, calls)
		{
			SemloomFilterCall *call = lfirst(call_cell);
			CustomPath *filter = semloom_make_filter_path(root, rel, rti, rte,
														child, call, downstream);

			if (lnext(calls, call_cell) != NULL)
			{
				filter->path.pathtarget = intermediate;
				/* The next input must not reuse this scan's computed input. */
				filter->flags &= ~CUSTOMPATH_SUPPORT_PROJECTION;
			}
			child = &filter->path;
			downstream = true;
		}
		semantic_paths = lappend(semantic_paths, child);
	}
	if (semantic_paths == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SemFilter lowering found no ordinary child path")));

	rel->pathlist = semantic_paths;
	rel->partial_pathlist = NIL;
}

static void
semloom_validate_filter_query_shape(PlannerInfo *root,
									FromExpr *source,
									Oid recording_oid,
									Oid exact_oid)
{
	Query *parse = root->parse;
	bool insert_source = semloom_is_insert_source(root);
	Query *insert = parse->commandType == CMD_INSERT ? parse :
		(insert_source ? root->parent_root->parse : NULL);
	Oid map_oid = semloom_map_function_oid();
	RangeTblRef *range_reference;
	RangeTblEntry *range_entry;

	if ((root->query_level != 1 && !insert_source) ||
		(parse->commandType != CMD_SELECT && parse->commandType != CMD_INSERT) ||
		parse->setOperations != NULL || parse->cteList != NIL || parse->hasAggs ||
		parse->groupClause != NIL || parse->groupingSets != NIL ||
		parse->havingQual != NULL || parse->hasWindowFuncs ||
		parse->windowClause != NIL || parse->distinctClause != NIL ||
		parse->rowMarks != NIL || parse->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("query shape is outside the current SemFilter capability"),
				 errdetail("Only a single-table SELECT or INSERT ... SELECT with ordinary filters, projections, ORDER BY, and LIMIT is supported.")));
	if (insert != NULL &&
		(insert->resultRelation == 0 || insert->onConflict != NULL ||
		 insert->returningList != NIL || insert->override != OVERRIDING_NOT_SET))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT shape is outside the current SemFilter capability"),
				 errdetail("ON CONFLICT, RETURNING, and OVERRIDING are not supported.")));
	if (list_length(source->fromlist) != 1 ||
		!IsA(linitial(source->fromlist), RangeTblRef))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the SemFilter capability requires one non-inherited table")));
	range_reference = linitial_node(RangeTblRef, source->fromlist);
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
						 SemloomFilterCall *call,
						 bool downstream)
{
	FuncExpr *marker = call->semantic->marker;
	CustomPath *path = makeNode(CustomPath);
	PathTarget *child_target;
	Path *projected_child = child_path;
	Node *input = (Node *) semloom_call_input(call->semantic);
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
	if (call->is_exact)
	{
		plan_private = call->choice_profile ?
			semloom_plan_spec_make_choice_filter_private(call->instruction, call->model_id, (AttrNumber) input_column) :
			semloom_plan_spec_make_exact_filter_private(call->instruction, call->model_id, (AttrNumber) input_column);
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
										   call->instruction,
										   &estimate);
			semloom_plan_spec_decode(plan_private,
								 CurrentMemoryContext,
								 &plan_spec,
								 &calibration_input_column);
			Assert(calibration_input_column == (AttrNumber) input_column);
			if (downstream)
			{
				double scale = child_path->rows / estimate.semantic_input_rows;

				estimate.semantic_input_rows = child_path->rows;
				estimate.estimated_model_calls *= scale;
				estimate.estimated_prompt_tokens *= scale;
				estimate.estimated_output_tokens *= scale;
				estimate.ai_work_cost *= scale;
				estimate.calibration_reason = "upstream-semantic-filter";
			}
			else
			{
				semloom_filter_calibration_load(
					&plan_spec,
					semloom_provider_execution_profile_name(),
					&calibration);
				semloom_filter_calibration_apply(&calibration, &estimate);
			}
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
	path->custom_restrictinfo = list_make1(call->restriction);
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
	/* Retain the call for PostgreSQL dependencies and native EXECUTE checks. */
	scan->custom_exprs = list_make1(copyObject(
		linitial_node(RestrictInfo, best_path->custom_restrictinfo)->clause));
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

		if (semloom_filter_marker_count((Node *) restriction->clause,
				semloom_filter_function_oid(), semloom_exact_filter_function_oid()) == 0)
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
	estimate->model_role = SEMLOOM_MODEL_REFERENCE_ROLE;
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
