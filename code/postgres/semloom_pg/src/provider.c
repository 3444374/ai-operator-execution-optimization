#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "utils/memutils.h"

#include "semloom_pg.h"

#define SEMLOOM_RECORDING_PROVIDER_NAME "in-process-recording"

struct SemloomProviderSession
{
	SemloomSemanticPlanSpec plan_spec;
	bool closed;
	uint64 accepted_rows;
	uint64 emitted_rows;
};

static Datum semloom_record_text(Datum input, MemoryContext result_context);

SemloomProviderSession *
semloom_provider_open(const SemloomSemanticPlanSpec *plan_spec)
{
	SemloomProviderSession *session;

	if (plan_spec == NULL || plan_spec->mapped_column <= 0 ||
		plan_spec->input_type != TEXTOID || plan_spec->output_type != TEXTOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid recording provider plan specification")));

	session = palloc0(sizeof(SemloomProviderSession));
	session->plan_spec = *plan_spec;
	return session;
}

void
semloom_provider_drive(SemloomProviderSession *session,
					   const SemloomPreparedSemanticTask *task,
					   MemoryContext result_context,
					   SemloomCompletionRecord *completion)
{
	if (session == NULL || session->closed || task == NULL || completion == NULL ||
		result_context == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("recording provider session is not open")));
	if (task->sequence != session->accepted_rows ||
		task->input_type != session->plan_spec.input_type)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("recording provider task does not match the open plan")));

	completion->sequence = task->sequence;
	completion->output_type = session->plan_spec.output_type;
	completion->is_null = task->is_null;
	completion->output = task->is_null ? (Datum) 0 :
		semloom_record_text(task->input, result_context);
	session->accepted_rows++;
	session->emitted_rows++;
}

void
semloom_provider_close(SemloomProviderSession *session)
{
	if (session != NULL)
		session->closed = true;
}

const char *
semloom_provider_name(const SemloomProviderSession *session)
{
	(void) session;
	return SEMLOOM_RECORDING_PROVIDER_NAME;
}

uint64
semloom_provider_accepted_rows(const SemloomProviderSession *session)
{
	return session == NULL ? 0 : session->accepted_rows;
}

uint64
semloom_provider_emitted_rows(const SemloomProviderSession *session)
{
	return session == NULL ? 0 : session->emitted_rows;
}

static Datum
semloom_record_text(Datum input, MemoryContext result_context)
{
	text *input_text = DatumGetTextPP(input);
	Size prefix_length = strlen(SEMLOOM_RECORDING_PREFIX);
	Size input_length = VARSIZE_ANY_EXHDR(input_text);
	MemoryContext previous_context;
	text *output_text;

	previous_context = MemoryContextSwitchTo(result_context);
	output_text = (text *) palloc(VARHDRSZ + prefix_length + input_length);
	SET_VARSIZE(output_text, VARHDRSZ + prefix_length + input_length);
	memcpy(VARDATA(output_text), SEMLOOM_RECORDING_PREFIX, prefix_length);
	memcpy(VARDATA(output_text) + prefix_length, VARDATA_ANY(input_text), input_length);
	MemoryContextSwitchTo(previous_context);

	return PointerGetDatum(output_text);
}
