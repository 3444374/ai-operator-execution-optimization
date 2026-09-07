/* Exercise versioned production carrier decoding with malformed PG nodes. */
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "planner/semantic_carrier.h"
#include "planner/semantic_binding.h"

PG_FUNCTION_INFO_V1(semloom_test_carrier);

Datum
semloom_test_carrier(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_PP(0));
	FuncExpr *marker = makeFuncExpr(1234, TEXTOID,
		list_make1(makeNullConst(TEXTOID, -1, InvalidOid)),
		InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	SemloomSemanticCall *call = semloom_call_create(1, SEMLOOM_CALL_FINAL_MAP, 0, 1, marker);
	List *fields = semloom_carrier_make_map(call,
		semloom_plan_spec_make_generate_map_fields("Echo.", "fixture-model", 128), 2,
		list_make1(list_make2(makeInteger(1), makeInteger(1))));
	SemloomPlanCarrier carrier;
	SemloomTupleBinding *binding;
	TupleDesc child = CreateTemplateTupleDesc(1);
	TupleDesc scan = CreateTemplateTupleDesc(2);
	List *entry;

	TupleDescInitEntry(child, 1, "input", TEXTOID, -1, 0);
	TupleDescInitEntry(scan, 1, "input", TEXTOID, -1, 0);
	TupleDescInitEntry(scan, 2, "result", TEXTOID, -1, 0);
	if (strcmp(mode, "version") == 0)
		lsecond(linitial_node(List, fields)) = makeInteger(2);
	else if (strcmp(mode, "unknown") == 0)
		linitial(linitial_node(List, fields)) = makeString("unknown");
	else if (strcmp(mode, "duplicate") == 0)
		lsecond(fields) = copyObject(linitial(fields));
	else if (strcmp(mode, "missing") == 0)
		fields = list_truncate(fields, 4);
	else if (strcmp(mode, "key") == 0)
		lsecond(lsecond_node(List, fields)) = list_make3(makeInteger(1), makeInteger(1), makeInteger(0));
	else if (strcmp(mode, "oid") == 0)
	{
		entry = lthird(fields);
		((Const *) lsecond(entry))->consttype = INT4OID;
	}
	else if (strcmp(mode, "semantic") == 0)
		lsecond((List *) list_nth(fields, 3)) = makeInteger(1);
	else if (strcmp(mode, "binding") == 0)
		lsecond((List *) list_nth(fields, 4)) = list_make1(makeInteger(2));
	else if (strcmp(mode, "result-type") == 0)
		TupleDescAttr(scan, 1)->atttypid = INT4OID;
	/* Both copyObject and the PG text roundtrip preserve the carrier representation. */
	fields = stringToNode(nodeToString(copyObject(fields)));
	semloom_carrier_decode(fields, CurrentMemoryContext, &carrier);
	binding = semloom_binding_projected(carrier.binding_fields, child, scan);
	PG_RETURN_BOOL(carrier.projected_input && !carrier.has_cost &&
		carrier.spec.marker_function_oid == 1234 && binding->input_column == 0 &&
		binding->result_column == 2 && binding->child_columns[0] == 1 &&
		strcmp(carrier.spec.instruction, "Echo.") == 0);
}
