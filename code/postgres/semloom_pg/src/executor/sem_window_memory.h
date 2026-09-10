/* Preflight sizes for bounded row materialization and conversion. */
#ifndef SEMLOOM_WINDOW_MEMORY_H
#define SEMLOOM_WINDOW_MEMORY_H
#include "postgres.h"
#include "executor/tuptable.h"

#define SEMLOOM_WINDOW_CONTEXT_ALLOWANCE (64 * 1024)
#define SEMLOOM_WINDOW_CONVERSION_LIMIT (1024 * 1024)

extern Size semloom_window_materialized_size(TupleTableSlot *slot);
extern Size semloom_window_row_allocation_bound(TupleTableSlot *slot,
	Size input_bytes, Size messages_bytes, bool has_result);
extern uint32 semloom_window_raw_text_bytes(Datum value);
#endif
