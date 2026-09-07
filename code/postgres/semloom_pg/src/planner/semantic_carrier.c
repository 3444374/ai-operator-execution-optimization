/* One discriminator for legacy plans and the projected-input Map carrier. */
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "planner/semantic_carrier.h"
#include "semantics/semantic_map_contract.h"

static List *
field(const char *name, void *value)
{
	return list_make2(makeString(pstrdup(name)), value);
}

List *
semloom_carrier_make_map(const SemloomSemanticCall *call, List *semantic_fields,
						 int result_column, List *mapping)
{
	Const *function = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
		ObjectIdGetDatum(call->marker->funcid), false, true);

	return list_make5(field("carrier_version", makeInteger(1)),
		field("call_key", semloom_call_key(call)), field("function_oid", function),
		field("semantic_fields", semantic_fields),
		field("binding", list_make2(makeInteger(result_column), mapping)));
}

pg_noreturn static void
invalid_carrier(void)
{
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		errmsg("invalid semantic plan carrier")));
	pg_unreachable();
}

void
semloom_carrier_decode(List *fields, MemoryContext owner, SemloomPlanCarrier *carrier)
{
	List *first;
	static const char *names[] = {"carrier_version", "call_key", "function_oid", "semantic_fields", "binding"};
	Node *values[lengthof(names)] = {0};
	ListCell *cell;
	List *key;
	Const *function;
	int index;

	MemSet(carrier, 0, sizeof(*carrier));
	if (fields == NIL || !IsA(fields, List) || !IsA(linitial(fields), List))
		invalid_carrier();
	first = linitial(fields);
	if (first == NIL)
		invalid_carrier();
	if (!IsA(linitial(first), String))
	{
		List *semantic = fields;

		carrier->has_cost = semloom_filter_cost_decode(fields, &carrier->cost);
		if (carrier->has_cost)
			semantic = list_make2(linitial(fields), lsecond(fields));
		semloom_plan_spec_decode(semantic, owner, &carrier->spec, &carrier->input_column);
		return;
	}
	if (list_length(fields) != lengthof(names))
		invalid_carrier();
	foreach(cell, fields)
	{
		List *entry = lfirst(cell);

		if (entry == NIL || !IsA(entry, List) || list_length(entry) != 2 ||
			!IsA(linitial(entry), String))
			invalid_carrier();
		for (index = 0; index < lengthof(names); index++)
			if (strcmp(strVal(linitial(entry)), names[index]) == 0)
				break;
		if (index == lengthof(names) || values[index] != NULL || lsecond(entry) == NULL)
			invalid_carrier();
		values[index] = lsecond(entry);
	}
	for (index = 0; index < lengthof(names); index++)
		if (values[index] == NULL)
			invalid_carrier();
	if (!IsA(values[0], Integer) || intVal(values[0]) != 1 ||
		!IsA(values[1], List) || !IsA(values[2], Const) ||
		!IsA(values[3], List) || !IsA(values[4], List))
		invalid_carrier();
	key = (List *) values[1];
	if (list_length(key) != 3 || !IsA(linitial(key), Integer) ||
		!IsA(lsecond(key), Integer) || !IsA(lthird(key), Integer) ||
		intVal(linitial(key)) <= 0 || intVal(lsecond(key)) != SEMLOOM_CALL_FINAL_MAP ||
		intVal(lthird(key)) != 0)
		invalid_carrier();
	function = (Const *) values[2];
	if (function->consttype != OIDOID || function->constisnull || !function->constbyval ||
		function->constlen != sizeof(Oid) || function->consttypmod != -1 ||
		OidIsValid(function->constcollid) || !OidIsValid(DatumGetObjectId(function->constvalue)))
		invalid_carrier();
	semloom_plan_spec_decode_fields((List *) values[3], owner, &carrier->spec);
	if (carrier->spec.schema_version != SEMLOOM_MAP_PLAN_SCHEMA_VERSION ||
		carrier->spec.operator_kind != SEMLOOM_PLAN_OPERATOR_MAP)
		invalid_carrier();
	carrier->spec.marker_function_oid = DatumGetObjectId(function->constvalue);
	carrier->projected_input = true;
	carrier->binding_fields = (List *) values[4];
}
