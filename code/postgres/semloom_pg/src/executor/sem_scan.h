/* PostgreSQL-visible scan identities and callback registrations. */
#ifndef SEMLOOM_SEM_SCAN_H
#define SEMLOOM_SEM_SCAN_H

#include "postgres.h"
#include "nodes/extensible.h"

#define SEMLOOM_MAP_CUSTOM_SCAN_NAME "SemLoom SemMap"
#define SEMLOOM_FILTER_CUSTOM_SCAN_NAME "SemLoom SemFilter"

extern const CustomScanMethods semloom_map_scan_methods;
extern const CustomScanMethods semloom_filter_scan_methods;

#endif
