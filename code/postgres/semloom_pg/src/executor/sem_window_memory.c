/* Measure before copying; allocator rounding is reserved separately from data. */
#include "postgres.h"
#include "access/detoast.h"
#include "utils/datum.h"
#include "utils/expandeddatum.h"
#include "utils/memutils.h"
#include "executor/sem_window_memory.h"
#include "semantics/semantic_map_contract.h"

static Size
bounded_add(Size a, Size b)
{
	if (a > MaxAllocSize || b > MaxAllocSize - a)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("semantic row exceeds supported allocation size")));
	return a + b;
}

Size
semloom_window_materialized_size(TupleTableSlot *slot)
{
	Size size = 0;
	int index;
	for (index = 0; index < slot->tts_tupleDescriptor->natts; index++)
	{
		Form_pg_attribute attribute = TupleDescAttr(slot->tts_tupleDescriptor, index);
		Datum value;
		Size bytes;
		if (attribute->attbyval || slot->tts_isnull[index]) continue;
		value = slot->tts_values[index];
		bytes = attribute->attlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)) ?
			EOH_get_flat_size(DatumGetEOHP(value)) : datumGetSize(value, false, attribute->attlen);
		/* MAXALIGN is conservative for every native attribute alignment. */
		size = bounded_add(MAXALIGN(size), bytes);
	}
	return size;
}

Size
semloom_window_row_allocation_bound(TupleTableSlot *slot,
	Size input_bytes, Size messages_bytes, bool has_result)
{
	Size bytes = semloom_window_materialized_size(slot);
	bytes = bounded_add(bytes, sizeof(VirtualTupleTableSlot) +
		(Size) slot->tts_tupleDescriptor->natts * (sizeof(Datum) + sizeof(bool)));
	bytes = bounded_add(bytes, input_bytes);
	bytes = bounded_add(bytes, messages_bytes);
	bytes = bounded_add(bytes, 2 * 257); /* bounded diagnostic ID and detoast copy */
	if (has_result) bytes = bounded_add(bytes, VARHDRSZ + SEMLOOM_MAP_MAX_OUTPUT_BYTES);
	/* At most six row allocations: small chunks round up, large chunks have
	 * fixed headers. Small AllocSet blocks and context headers fit the allowance. */
	return bounded_add(bounded_add(bytes, bytes), SEMLOOM_WINDOW_CONTEXT_ALLOWANCE);
}

uint32
semloom_window_raw_text_bytes(Datum value)
{
	Size raw = toast_raw_datum_size(value);
	if (raw < VARHDRSZ || raw - VARHDRSZ > PG_UINT32_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			errmsg("semantic input exceeds supported representation size")));
	return (uint32) (raw - VARHDRSZ);
}
