/* Map call analysis; returned expressions are borrowed from the supplied Query. */
#ifndef SEMLOOM_MAP_CALL_H
#define SEMLOOM_MAP_CALL_H

#include "postgres.h"
#include "nodes/parsenodes.h"

/* Before planning: validate fixed arguments/placement and return the source level. */
extern int semloom_validate_generate_map_source(Query *parse);
/* At the current planning stage: identify and validate one visible Map occurrence. */
extern Oid semloom_map_marker_oid(Query *parse);
extern FuncExpr *semloom_map_call(Query *parse, Oid marker_oid);

#endif
