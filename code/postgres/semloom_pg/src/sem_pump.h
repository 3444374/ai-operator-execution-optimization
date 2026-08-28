#ifndef SEMLOOM_SEM_PUMP_H
#define SEMLOOM_SEM_PUMP_H

#include "postgres.h"

#include "commands/explain.h"
#include "nodes/extensible.h"

typedef struct SemloomExecPump SemloomExecPump;

extern SemloomExecPump *semloom_pump_begin(CustomScanState *node,
											EState *estate,
											int executor_flags);
extern TupleTableSlot *semloom_pump_next(SemloomExecPump *pump,
										 ScanState *scan_state);
extern void semloom_pump_stop(SemloomExecPump *pump, CustomScanState *node);
extern void semloom_pump_explain(const SemloomExecPump *pump,
									 ExplainState *explain_state);

#endif
