/* Unary semantic path builders and scoped source-validation state. */
#ifndef SEMLOOM_PATHS_H
#define SEMLOOM_PATHS_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"

extern int semloom_validate_generate_map_source(Query *parse);
extern bool semloom_generate_map_source_checked(int query_level);
extern void semloom_add_sem_map_paths(PlannerInfo *root,
									 UpperRelationKind stage,
									 RelOptInfo *input_rel,
									 RelOptInfo *output_rel);
extern void semloom_add_sem_filter_paths(PlannerInfo *root,
									 RelOptInfo *rel,
									 Index rti,
									 RangeTblEntry *rte);

#endif
