/* Test-only Map plan codec calls and PostgreSQL callback observations. */
#include "postgres.h"

#include "catalog/objectaccess.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"

#include "sem_plan_spec.h"

PG_FUNCTION_INFO_V1(semloom_test_map_plan);
PG_FUNCTION_INFO_V1(semloom_test_map_watch);
PG_FUNCTION_INFO_V1(semloom_test_map_events);

static object_access_hook_type previous_access_hook;
static planner_hook_type previous_planner_hook;
static Oid watched_map;
static Oid watched_child;
static uint32 map_executions;
static uint32 child_initializations;
static uint32 map_plans;

void _PG_init(void);
void _PG_fini(void);

static bool
contains_watched_map(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == watched_map)
		return true;
	if (IsA(node, Query))
		return query_tree_walker((Query *) node, contains_watched_map, context, 0);
	return expression_tree_walker(node, contains_watched_map, context);
}

static PlannedStmt *
observe_map_plan(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params)
{
	if (OidIsValid(watched_map) && contains_watched_map((Node *) parse, NULL))
		map_plans++;
	if (previous_planner_hook != NULL)
		return previous_planner_hook(parse, query_string, cursor_options, bound_params);
	return standard_planner(parse, query_string, cursor_options, bound_params);
}

static void
observe_function_access(ObjectAccessType access, Oid class_id, Oid object_id, int sub_id, void *argument)
{
	if (access == OAT_FUNCTION_EXECUTE && class_id == ProcedureRelationId)
	{
		if (OidIsValid(watched_map) && object_id == watched_map)
			map_executions++;
		if (OidIsValid(watched_child) && object_id == watched_child)
			child_initializations++;
	}
	if (previous_access_hook != NULL)
		previous_access_hook(access, class_id, object_id, sub_id, argument);
}

void
_PG_init(void)
{
	previous_access_hook = object_access_hook;
	object_access_hook = observe_function_access;
	previous_planner_hook = planner_hook;
	planner_hook = observe_map_plan;
}

void
_PG_fini(void)
{
	if (object_access_hook == observe_function_access)
		object_access_hook = previous_access_hook;
	if (planner_hook == observe_map_plan)
		planner_hook = previous_planner_hook;
}

Datum
semloom_test_map_watch(PG_FUNCTION_ARGS)
{
	watched_map = PG_GETARG_OID(0);
	watched_child = PG_GETARG_OID(1);
	map_executions = child_initializations = map_plans = 0;
	PG_RETURN_VOID();
}

Datum
semloom_test_map_events(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%u|%u|%u",
		map_executions, child_initializations, map_plans)));
}

static List *
map_field(List *fields, const char *name)
{
	ListCell *cell;

	foreach(cell, fields)
	{
		List *field = lfirst(cell);

		if (strcmp(strVal(linitial(field)), name) == 0)
			return field;
	}
	elog(ERROR, "unknown test mutation field");
	return NIL;
}

Datum
semloom_test_map_plan(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_PP(0));
	Oid function_oid = PG_GETARG_OID(1);
	MemoryContext result_context = CurrentMemoryContext;
	MemoryContext source_context = AllocSetContextCreate(result_context,
		"map plan source", ALLOCSET_DEFAULT_SIZES);
	MemoryContext copy_context = AllocSetContextCreate(result_context,
		"map plan copy", ALLOCSET_DEFAULT_SIZES);
	List *original;
	List *copied;
	List *fields;
	List *binding;
	SemloomPlanSpec decoded;
	AttrNumber column;

	MemoryContextSwitchTo(source_context);
	original = semloom_plan_spec_make_generate_map_private("Echo the input.", "golden-map-v1", 128, 1, function_oid);
	MemoryContextSwitchTo(copy_context);
	copied = copyObject(original);
	MemoryContextDelete(source_context);
	fields = linitial(copied);
	binding = lsecond(copied);
	if (strcmp(mode, "column") == 0)
		linitial(binding) = makeInteger(2);
	else if (strcmp(mode, "duplicate") == 0)
		lsecond(fields) = copyObject(linitial(fields));
	else if (strcmp(mode, "missing") == 0)
		linitial(copied) = list_delete_ptr(fields, map_field(fields, "max_output_bytes"));
	else if (strcmp(mode, "unknown") == 0)
		linitial(map_field(fields, "max_output_bytes")) = makeString(pstrdup("unknown"));
	else if (strcmp(mode, "binding-type") == 0)
		lsecond(copied) = makeInteger(1);
	else if (strcmp(mode, "binding-column") == 0)
		linitial(binding) = makeInteger(65537);
	else if (strcmp(mode, "binding-function-type") == 0)
		lsecond_node(Const, binding)->consttype = INT4OID;
	else if (strcmp(mode, "binding-function-null") == 0)
		lsecond_node(Const, binding)->constisnull = true;
	else if (strcmp(mode, "binding-function-zero") == 0)
		lsecond_node(Const, binding)->constvalue = ObjectIdGetDatum(InvalidOid);
	else if (strcmp(mode, "binding-function-byref") == 0)
		lsecond_node(Const, binding)->constbyval = false;
	else if (strncmp(mode, "field:", 6) == 0)
	{
		List *field = map_field(fields, mode + 6);
		Node *value = lsecond(field);

		if (IsA(value, Integer))
			lsecond(field) = makeInteger(intVal(value) + 1);
		else
			lsecond(field) = makeString(pstrdup("changed"));
	}
	else if (strcmp(mode, "copy") != 0)
		elog(ERROR, "unknown Map plan test case");
	MemoryContextSwitchTo(result_context);
	semloom_plan_spec_decode(copied, result_context, &decoded, &column);
	MemoryContextDelete(copy_context);
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%u|%d|%u|%s|%s|%u|%u|%u|%s|%s|%s",
		decoded.schema_version, column, decoded.marker_function_oid,
		decoded.semantic_spec_digest, decoded.physical_algorithm_digest,
		decoded.max_tokens, decoded.max_input_bytes, decoded.max_output_bytes,
		decoded.has_stop || decoded.stop != NULL ? "stop" : "absent",
		decoded.model_id, decoded.instruction)));
}
