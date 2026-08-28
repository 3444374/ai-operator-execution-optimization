#ifndef SEMLOOM_PG_H
#define SEMLOOM_PG_H

#include "postgres.h"

#include "nodes/extensible.h"
#include "nodes/pathnodes.h"

#define SEMLOOM_CUSTOM_SCAN_NAME "SemLoom SemMap"

extern const CustomScanMethods semloom_scan_methods;

extern const char *semloom_gateway_socket_path(void);
extern Oid semloom_map_function_oid(void);
extern bool semloom_is_map_function(Oid function_oid);
extern void semloom_add_sem_map_paths(PlannerInfo *root,
									 UpperRelationKind stage,
									 RelOptInfo *input_rel,
									 RelOptInfo *output_rel);

#endif
