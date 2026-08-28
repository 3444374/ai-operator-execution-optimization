#ifndef SEMLOOM_PG_H
#define SEMLOOM_PG_H

#include "postgres.h"

#include "nodes/extensible.h"
#include "nodes/pathnodes.h"

#define SEMLOOM_CUSTOM_SCAN_NAME "SemLoom SemMap"
#define SEMLOOM_RECORDING_PREFIX "recorded:"

typedef struct SemloomProviderSession SemloomProviderSession;

typedef struct SemloomSemanticPlanSpec
{
	AttrNumber mapped_column;
	Oid input_type;
	Oid output_type;
} SemloomSemanticPlanSpec;

typedef struct SemloomPreparedSemanticTask
{
	uint64 sequence;
	Oid input_type;
	Datum input;
	bool is_null;
} SemloomPreparedSemanticTask;

typedef struct SemloomCompletionRecord
{
	uint64 sequence;
	Oid output_type;
	Datum output;
	bool is_null;
} SemloomCompletionRecord;

extern const CustomScanMethods semloom_scan_methods;

extern Oid semloom_map_function_oid(void);
extern bool semloom_is_map_function(Oid function_oid);
extern SemloomProviderSession *semloom_provider_open(const SemloomSemanticPlanSpec *plan_spec);
extern void semloom_provider_drive(SemloomProviderSession *session,
								   const SemloomPreparedSemanticTask *task,
								   MemoryContext result_context,
								   SemloomCompletionRecord *completion);
extern void semloom_provider_close(SemloomProviderSession *session);
extern const char *semloom_provider_name(const SemloomProviderSession *session);
extern uint64 semloom_provider_accepted_rows(const SemloomProviderSession *session);
extern uint64 semloom_provider_emitted_rows(const SemloomProviderSession *session);
extern void semloom_add_sem_map_paths(PlannerInfo *root,
									 UpperRelationKind stage,
									 RelOptInfo *input_rel,
									 RelOptInfo *output_rel);

#endif
