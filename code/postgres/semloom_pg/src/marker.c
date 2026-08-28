#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(semloom_marker_map);

Datum
semloom_marker_map(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("ai_semantic.map marker was not lowered to a semantic plan"),
			 errhint("Load semloom_pg before planning and use a supported SemMap query shape.")));

	PG_RETURN_NULL();
}
