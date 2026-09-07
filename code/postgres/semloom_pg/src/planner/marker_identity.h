/* PostgreSQL function identity shared by planning and executor validation. */
#ifndef SEMLOOM_MARKER_IDENTITY_H
#define SEMLOOM_MARKER_IDENTITY_H

#include "postgres.h"

extern Oid semloom_map_function_oid(void);
extern Oid semloom_generate_map_function_oid(void);
extern Oid semloom_filter_function_oid(void);
extern Oid semloom_exact_filter_function_oid(void);
extern bool semloom_is_map_function(Oid function_oid);

#endif
