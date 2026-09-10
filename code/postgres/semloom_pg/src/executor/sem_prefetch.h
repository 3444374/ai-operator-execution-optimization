/* Conservative demand analysis for the first predicate-prefetch capability. */
#ifndef SEMLOOM_PREFETCH_H
#define SEMLOOM_PREFETCH_H

#include "nodes/plannodes.h"

extern const char *semloom_prefetch_reason(Plan *plan, Node *input, bool allow_predicates);

#endif
