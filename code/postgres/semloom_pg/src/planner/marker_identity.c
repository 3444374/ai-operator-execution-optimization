/* Resolve only marker functions owned by this extension. */
#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type_d.h"
#include "commands/extension.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "parser/parse_func.h"

#include "planner/marker_identity.h"

static Oid
semloom_lookup_marker(const char *name, int nargs, const Oid *argument_types)
{
	Oid extension_oid = get_extension_oid("semloom_pg", true);
	Oid function_oid;
	List *qualified_name;

	if (!OidIsValid(extension_oid))
		return InvalidOid;

	qualified_name = list_make2(makeString(pstrdup("ai_semantic")),
								makeString(pstrdup(name)));
	function_oid = LookupFuncName(qualified_name, nargs, argument_types, true);
	list_free_deep(qualified_name);
	if (!OidIsValid(function_oid) ||
		getExtensionOfObject(ProcedureRelationId, function_oid) != extension_oid)
		return InvalidOid;

	return function_oid;
}

Oid
semloom_map_function_oid(void)
{
	Oid argument_types[1] = {TEXTOID};

	return semloom_lookup_marker("map", lengthof(argument_types), argument_types);
}

Oid
semloom_generate_map_function_oid(void)
{
	Oid argument_types[3] = {TEXTOID, TEXTOID, JSONBOID};

	return semloom_lookup_marker("map", lengthof(argument_types), argument_types);
}

Oid
semloom_filter_function_oid(void)
{
	Oid argument_types[1] = {TEXTOID};

	return semloom_lookup_marker("filter", lengthof(argument_types), argument_types);
}

Oid
semloom_exact_filter_function_oid(void)
{
	Oid argument_types[3] = {TEXTOID, TEXTOID, JSONBOID};

	return semloom_lookup_marker("filter", lengthof(argument_types), argument_types);
}

bool
semloom_is_map_function(Oid function_oid)
{
	Oid marker_oid = semloom_map_function_oid();
	Oid generate_oid = semloom_generate_map_function_oid();

	return (OidIsValid(marker_oid) && function_oid == marker_oid) ||
		(OidIsValid(generate_oid) && function_oid == generate_oid);
}
