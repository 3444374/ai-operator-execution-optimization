/* Test-only callers of the production copyObject-safe plan codec. */
#include "postgres.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "generation_profile.h"
#include "sem_plan_spec.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(semloom_test_plan);

static List *
named_field(List *fields, const char *name)
{
	ListCell *cell;

	foreach(cell, fields)
	{
		List *field = lfirst(cell);

		if (strcmp(strVal(linitial(field)), name) == 0)
			return field;
	}
	elog(ERROR, "test fixture field not found");
	return NIL;
}

Datum
semloom_test_plan(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_PP(0));
	MemoryContext result_context = CurrentMemoryContext;
	MemoryContext source_context = AllocSetContextCreate(result_context,
		"test plan source", ALLOCSET_DEFAULT_SIZES);
	MemoryContext copy_context = AllocSetContextCreate(result_context,
		"test plan copy", ALLOCSET_DEFAULT_SIZES);
	List *original;
	List *copied;
	List *fields;
	List *profile_field;
	List *profile;
	SemloomPlanSpec decoded;
	AttrNumber binding;
	uint8 encoded[SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES];
	uint32 written;

	MemoryContextSwitchTo(source_context);
	original = strcmp(mode, "old") == 0 ?
		semloom_plan_spec_make_exact_filter_private("Classify input.", "golden-model-v1", 1) :
		semloom_plan_spec_make_choice_filter_private("Classify input.", "golden-model-v1", 1);
	MemoryContextSwitchTo(copy_context);
	copied = copyObject(original);
	MemoryContextDelete(source_context);
	fields = linitial(copied);
	if (strcmp(mode, "old") != 0)
	{
		profile_field = named_field(fields, "generation_profile");
		profile = lsecond(profile_field);
		if (strcmp(mode, "missing-profile") == 0)
			linitial(copied) = list_delete_ptr(fields, profile_field);
		else if (strcmp(mode, "old-with-profile") == 0)
			lsecond(named_field(fields, "schema_version")) = makeInteger(2);
		else if (strcmp(mode, "future-schema") == 0)
			lsecond(named_field(fields, "schema_version")) = makeInteger(4);
		else if (strcmp(mode, "binding") == 0)
			lsecond(copied) = makeInteger(2);
		else if (strcmp(mode, "binding-overflow") == 0)
			lsecond(copied) = makeInteger(65537);
		else if (strcmp(mode, "outer-digest") == 0)
			lsecond(named_field(fields, "semantic_spec_digest")) = makeString(pstrdup(
				"9ec789eab10d6367b60895288fde154b384edeba1ac0fb603ade0b2424ff2fb9"));
		else if (strcmp(mode, "null-profile") == 0)
			lsecond(profile_field) = NULL;
		else if (strcmp(mode, "extra") == 0)
			lsecond(profile_field) = lappend(profile, copyObject(linitial(profile)));
		else if (strcmp(mode, "missing") == 0)
			lsecond(profile_field) = list_delete_first(profile);
		else if (strcmp(mode, "duplicate") == 0)
			lsecond(profile) = copyObject(linitial(profile));
		else if (strcmp(mode, "unknown-field") == 0)
			linitial(linitial_node(List, profile)) = makeString(pstrdup("unknown"));
		else if (strcmp(mode, "unknown-id") == 0)
			lsecond(named_field(profile, "profile_id")) = makeString(pstrdup("unknown"));
		else if (strcmp(mode, "oversized-id") == 0)
		{
			char *value = palloc0(130);

			memset(value, 'x', 129);
			lsecond(named_field(profile, "profile_id")) = makeString(value);
		}
		else if (strcmp(mode, "unknown-version") == 0)
			lsecond(named_field(profile, "profile_version")) = makeInteger(2);
		else if (strcmp(mode, "version-type") == 0)
			lsecond(named_field(profile, "profile_version")) = makeString(pstrdup("1"));
		else if (strcmp(mode, "constraint") == 0)
			lsecond(named_field(profile, "constraint_kind")) = makeString(pstrdup("JSON"));
		else if (strcmp(mode, "digest") == 0)
			lsecond(named_field(profile, "profile_digest")) = makeString(pstrdup("0000"));
		else if (strcmp(mode, "choices-type") == 0)
			lsecond(named_field(profile, "choices")) = makeInteger(3);
		else if (strcmp(mode, "choice-type") == 0 || strcmp(mode, "choice-content") == 0 ||
				 strcmp(mode, "choice-order") == 0 || strcmp(mode, "choice-count") == 0)
		{
			List *choice_field = named_field(profile, "choices");
			List *choices = lsecond(choice_field);

			if (strcmp(mode, "choice-type") == 0)
				linitial(choices) = makeInteger(1);
			else if (strcmp(mode, "choice-content") == 0)
				linitial(choices) = makeString(pstrdup("true"));
			else if (strcmp(mode, "choice-count") == 0)
				lsecond(choice_field) = list_delete_first(choices);
			else
			{
				void *first = linitial(choices);

				linitial(choices) = lsecond(choices);
				lsecond(choices) = first;
			}
		}
	}
	MemoryContextSwitchTo(result_context);
	semloom_plan_spec_decode(copied, result_context, &decoded, &binding);
	MemoryContextDelete(copy_context);
	/* All strings/slices must survive both source and copied tree destruction. */
	if (decoded.generation_profile_digest != NULL &&
		!semloom_generation_profile_encode(&decoded.generation_profile,
			encoded, sizeof(encoded), &written))
		elog(ERROR, "decoded profile did not survive its plan tree");
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%u|%d|%s|%s|%s", decoded.schema_version,
		binding, decoded.semantic_spec_digest, decoded.physical_algorithm_digest,
		decoded.generation_profile_digest == NULL ? "absent" : decoded.generation_profile_digest)));
}
