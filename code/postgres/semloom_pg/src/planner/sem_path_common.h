/* Shared planner helpers for unary semantic markers. */
#ifndef SEMLOOM_SEM_PATH_COMMON_H
#define SEMLOOM_SEM_PATH_COMMON_H

#include "postgres.h"

#include "nodes/pathnodes.h"

extern int semloom_marker_count(Node *node, Oid marker_oid);
extern bool semloom_is_insert_source(PlannerInfo *root);

#endif
