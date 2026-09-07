/* Exercise production call ownership and tuple bindings in a PostgreSQL backend. */
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "planner/semantic_binding.h"
#include "planner/semantic_call.h"

PG_FUNCTION_INFO_V1(semloom_test_binding);
PG_FUNCTION_INFO_V1(semloom_test_call);
PG_FUNCTION_INFO_V1(semloom_test_binding_setrefs);

static TupleDesc
make_descriptor(Oid first, Oid second, bool result)
{
	TupleDesc descriptor = CreateTemplateTupleDesc(result ? 3 : 2);

	TupleDescInitEntry(descriptor, 1, "first", first, -1, 0);
	TupleDescInitEntry(descriptor, 2, "second", second, -1, 0);
	if (result)
		TupleDescInitEntry(descriptor, 3, "result", second, -1, 0);
	return descriptor;
}

Datum
semloom_test_binding(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool binary = strcmp(mode, "binary") == 0;
	bool legacy = strncmp(mode, "legacy", 6) == 0;
	Oid payload_type = binary ? BYTEAOID : TEXTOID;
	TupleDesc child = make_descriptor(payload_type, INT4OID, false);
	TupleDesc scan = legacy ? make_descriptor(payload_type, INT4OID, false) :
		make_descriptor(INT4OID, payload_type, true);
	List *mapping = list_make2(list_make2(makeInteger(1), makeInteger(2)),
		list_make2(makeInteger(2), makeInteger(1)));
	List *fields = list_make3(makeInteger(1), makeInteger(3), mapping);
	MemoryContext parent = CurrentMemoryContext;
	MemoryContext temporary = AllocSetContextCreate(parent, "binding source", ALLOCSET_SMALL_SIZES);
	SemloomTupleBinding *binding;
	TupleTableSlot *input;
	TupleTableSlot *output;
	text *payload = cstring_to_text_with_len("a\0b", 3);
	bool is_null = strcmp(mode, "null") == 0;
	bool passed;

	if (strcmp(mode, "input-range") == 0)
		linitial(fields) = makeInteger(3);
	else if (strcmp(mode, "input-overflow") == 0)
		linitial(fields) = makeInteger(65537);
	else if (strcmp(mode, "input-type") == 0)
		linitial(fields) = makeString("1");
	else if (strcmp(mode, "result-range") == 0)
		lsecond(fields) = makeInteger(4);
	else if (strcmp(mode, "result-overlap") == 0)
		lsecond(fields) = makeInteger(2);
	else if (strcmp(mode, "duplicate-target") == 0)
		lsecond(lsecond_node(List, mapping)) = makeInteger(2);
	else if (strcmp(mode, "missing-target") == 0)
		lthird(fields) = list_make1(linitial(mapping));
	else if (strcmp(mode, "malformed-pair") == 0)
		linitial(mapping) = makeInteger(1);
	else if (strcmp(mode, "malformed-list") == 0)
		lthird(fields) = list_make1_int(1);
	else if (strcmp(mode, "unknown-field") == 0)
		fields = lappend(fields, makeInteger(0));
	else if (strcmp(mode, "type-mismatch") == 0)
		TupleDescAttr(scan, 0)->atttypid = INT8OID;
	else if (strcmp(mode, "typmod-mismatch") == 0)
		TupleDescAttr(scan, 1)->atttypmod = 8;
	else if (strcmp(mode, "collation-mismatch") == 0)
		TupleDescAttr(scan, 1)->attcollation = InvalidOid;
	else if (strcmp(mode, "dropped-input") == 0)
		TupleDescAttr(child, 0)->attisdropped = true;
	else if (strcmp(mode, "dropped-result") == 0)
		TupleDescAttr(scan, 2)->attisdropped = true;

	MemoryContextSwitchTo(temporary);
	fields = copyObject(fields);
	MemoryContextSwitchTo(parent);
	binding = legacy ? semloom_binding_legacy(1, strcmp(mode, "legacy-filter") != 0, child, scan) :
		semloom_binding_decode(fields, child, scan);
	MemoryContextDelete(temporary);
	input = MakeSingleTupleTableSlot(child, &TTSOpsVirtual);
	output = MakeSingleTupleTableSlot(scan, &TTSOpsVirtual);
	input->tts_values[0] = PointerGetDatum(payload);
	input->tts_isnull[0] = is_null;
	input->tts_values[1] = Int32GetDatum(17);
	input->tts_isnull[1] = false;
	ExecStoreVirtualTuple(input);
	semloom_binding_store(binding, input, output);
	passed = output->tts_values[legacy ? 0 : 1] == input->tts_values[0] &&
		output->tts_isnull[legacy ? 0 : 1] == is_null &&
		DatumGetInt32(output->tts_values[legacy ? 1 : 0]) == 17 &&
		binding->input_column == 1;
	if (!legacy)
	{
		passed = passed && binding->result_column == 3 && output->tts_isnull[2];
		output->tts_values[2] = PointerGetDatum(cstring_to_text("generated"));
		output->tts_isnull[2] = false;
		passed = passed && output->tts_values[1] == PointerGetDatum(payload);
		if (strcmp(mode, "reuse") == 0)
		{
			input->tts_isnull[0] = true;
			semloom_binding_store(binding, input, output);
			passed = passed && output->tts_isnull[1] && output->tts_isnull[2];
		}
	}
	else
		passed = passed && binding->result_column == (strcmp(mode, "legacy-filter") == 0 ? 0 : 1);
	ExecDropSingleTupleTableSlot(input);
	ExecDropSingleTupleTableSlot(output);
	PG_RETURN_BOOL(passed);
}

Datum
semloom_test_call(PG_FUNCTION_ARGS)
{
	MemoryContext parent = CurrentMemoryContext;
	MemoryContext source = AllocSetContextCreate(parent, "call source", ALLOCSET_SMALL_SIZES);
	FuncExpr *marker;
	SemloomSemanticCall *first;
	SemloomSemanticCall *second;
	SemloomSemanticCall *map;
	List *key;

	MemoryContextSwitchTo(source);
	marker = makeFuncExpr(1234, TEXTOID, list_make1(makeNullConst(TEXTOID, -1, InvalidOid)),
		InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	MemoryContextSwitchTo(parent);
	first = semloom_call_create(1, SEMLOOM_CALL_BASE_FILTER, 0, 1, marker);
	second = semloom_call_create(1, SEMLOOM_CALL_BASE_FILTER, 1, 2, marker);
	map = semloom_call_create(1, SEMLOOM_CALL_FINAL_MAP, 0, 1, marker);
	MemoryContextDelete(source);
	key = copyObject(semloom_call_key(first));
	PG_RETURN_BOOL(first->marker != second->marker && equal(first->marker, second->marker) &&
		!equal(key, semloom_call_key(second)) && !equal(key, semloom_call_key(map)) &&
		IsA(semloom_call_input(first), Const) && second->source_locator == 2);
}

/* A logical marker matching the result descriptor must not carry ACL initialization alone. */
Datum
semloom_test_binding_setrefs(PG_FUNCTION_ARGS)
{
	Oid function = PG_GETARG_OID(0);
	PlannerInfo *root = makeNode(PlannerInfo);
	CustomScan *scan = makeNode(CustomScan);
	Result *child = makeNode(Result);
	FuncExpr *marker = makeFuncExpr(function, TEXTOID,
		list_make3(makeNullConst(TEXTOID, -1, InvalidOid),
			makeNullConst(TEXTOID, -1, InvalidOid), makeNullConst(JSONBOID, -1, InvalidOid)),
		InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	Var *result;
	Node *retained;

	root->glob = makeNode(PlannerGlobal);
	root->parse = makeNode(Query);
	root->query_level = 1;
	child->plan.targetlist = list_make1(makeTargetEntry(
		(Expr *) makeNullConst(TEXTOID, -1, InvalidOid), 1, NULL, false));
	scan->custom_plans = list_make1(child);
	scan->custom_scan_tlist = lappend(copyObject(child->plan.targetlist),
		makeTargetEntry((Expr *) copyObject(marker), 2, NULL, false));
	scan->scan.plan.targetlist = list_make1(makeTargetEntry((Expr *) marker, 1, NULL, false));
	scan->custom_exprs = list_make1(copyObject(marker));
	scan = (CustomScan *) set_plan_references(root, (Plan *) scan);
	result = (Var *) linitial_node(TargetEntry, scan->scan.plan.targetlist)->expr;
	retained = linitial(scan->custom_exprs);
	PG_RETURN_BOOL(IsA(result, Var) && result->varno == INDEX_VAR && result->varattno == 2 &&
		IsA(retained, Var) && ((Var *) retained)->varno == INDEX_VAR &&
		((Var *) retained)->varattno == 2 && root->glob->invalItems != NIL);
}
