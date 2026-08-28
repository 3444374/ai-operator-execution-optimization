CREATE SCHEMA ai_semantic;

CREATE FUNCTION ai_semantic.map(input text)
RETURNS text
AS 'MODULE_PATHNAME', 'semloom_marker_map'
LANGUAGE C
VOLATILE
PARALLEL UNSAFE;

COMMENT ON FUNCTION ai_semantic.map(text) IS
'Fail-closed SemMap marker; supported statements are lowered to a SemLoom CustomScan';

-- A planner hook must be present before the first marker statement is planned.
LOAD 'MODULE_PATHNAME';
