/* Validated tuple routing; zero in the result position means no child value. */
#ifndef SEMLOOM_SEMANTIC_BINDING_H
#define SEMLOOM_SEMANTIC_BINDING_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"

typedef struct SemloomTupleBinding
{
	AttrNumber input_column;
	AttrNumber result_column;
	int child_natts;
	int scan_natts;
	AttrNumber *child_columns;
} SemloomTupleBinding;

/* V1: [input, result-or-zero, [[child, scan], ...]]. All values are PG Nodes. */
extern SemloomTupleBinding *semloom_binding_decode(List *fields,
	TupleDesc child, TupleDesc scan);
/* Only old carriers may overwrite input; they retain the original tuple layout. */
extern SemloomTupleBinding *semloom_binding_legacy(AttrNumber input_column,
	bool produces_result, TupleDesc child, TupleDesc scan);
extern void semloom_binding_store(const SemloomTupleBinding *binding,
	TupleTableSlot *child, TupleTableSlot *scan);

#endif
