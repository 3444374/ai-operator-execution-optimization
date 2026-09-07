/* Decode and validate routing once, then copy borrowed Datums without re-inferring columns. */
#include "postgres.h"
#include "nodes/value.h"
#include "planner/semantic_binding.h"

pg_noreturn static void invalid_binding(void);

static int
read_column(Node *value, int maximum, bool allow_zero)
{
	int column;

	if (value == NULL || !IsA(value, Integer))
		invalid_binding();
	column = intVal(value);
	if (column < (allow_zero ? 0 : 1) || column > maximum)
		invalid_binding();
	return column;
}

static SemloomTupleBinding *
allocate_binding(TupleDesc child, TupleDesc scan)
{
	SemloomTupleBinding *binding;

	if (child->natts <= 0 || child->natts > PG_INT16_MAX ||
		scan->natts <= 0 || scan->natts > PG_INT16_MAX)
		invalid_binding();
	binding = palloc0(sizeof(*binding));
	binding->child_natts = child->natts;
	binding->scan_natts = scan->natts;
	binding->child_columns = palloc0(sizeof(AttrNumber) * scan->natts);
	return binding;
}

static void
validate_passthrough(TupleDesc child, int source, TupleDesc scan, int target)
{
	Form_pg_attribute a = TupleDescAttr(child, source - 1);
	Form_pg_attribute b = TupleDescAttr(scan, target - 1);

	if (a->attisdropped || b->attisdropped || a->atttypid != b->atttypid ||
		a->atttypmod != b->atttypmod || a->attcollation != b->attcollation)
		invalid_binding();
}

SemloomTupleBinding *
semloom_binding_decode(List *fields, TupleDesc child, TupleDesc scan)
{
	SemloomTupleBinding *binding = allocate_binding(child, scan);
	List *mapping;
	ListCell *cell;
	int column;

	if (fields == NIL || !IsA(fields, List) || list_length(fields) != 3)
		invalid_binding();
	binding->input_column = read_column(linitial(fields), child->natts, false);
	binding->result_column = read_column(lsecond(fields), scan->natts, true);
	if (TupleDescAttr(child, binding->input_column - 1)->attisdropped)
		invalid_binding();
	mapping = lthird(fields);
	if (mapping != NIL && !IsA(mapping, List))
		invalid_binding();
	foreach(cell, mapping)
	{
		List *pair = lfirst(cell);
		int source;
		int target;

		if (pair == NIL || !IsA(pair, List) || list_length(pair) != 2)
			invalid_binding();
		source = read_column(linitial(pair), child->natts, false);
		target = read_column(lsecond(pair), scan->natts, false);
		if (target == binding->result_column || binding->child_columns[target - 1] != 0)
			invalid_binding();
		validate_passthrough(child, source, scan, target);
		binding->child_columns[target - 1] = source;
	}
	for (column = 1; column <= scan->natts; column++)
	{
		if (column == binding->result_column)
		{
			if (TupleDescAttr(scan, column - 1)->attisdropped)
				invalid_binding();
		}
		else if (binding->child_columns[column - 1] == 0)
			invalid_binding();
	}
	return binding;
}

SemloomTupleBinding *
semloom_binding_legacy(AttrNumber input_column, bool produces_result,
					   TupleDesc child, TupleDesc scan)
{
	SemloomTupleBinding *binding = allocate_binding(child, scan);
	int column;

	if (child->natts != scan->natts || input_column <= 0 || input_column > child->natts)
		invalid_binding();
	binding->input_column = input_column;
	binding->result_column = produces_result ? input_column : 0;
	for (column = 1; column <= scan->natts; column++)
	{
		validate_passthrough(child, column, scan, column);
		binding->child_columns[column - 1] = column;
	}
	return binding;
}

void
semloom_binding_store(const SemloomTupleBinding *binding, TupleTableSlot *child,
					  TupleTableSlot *scan)
{
	int column;

	if (child->tts_tupleDescriptor->natts != binding->child_natts ||
		scan->tts_tupleDescriptor->natts != binding->scan_natts)
		invalid_binding();
	slot_getallattrs(child);
	ExecClearTuple(scan);
	for (column = 0; column < binding->scan_natts; column++)
	{
		AttrNumber source = binding->child_columns[column];

		scan->tts_isnull[column] = source == 0 || child->tts_isnull[source - 1];
		scan->tts_values[column] = source == 0 ? (Datum) 0 : child->tts_values[source - 1];
	}
}

static void
invalid_binding(void)
{
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		errmsg("invalid semantic tuple binding")));
	pg_unreachable();
}
