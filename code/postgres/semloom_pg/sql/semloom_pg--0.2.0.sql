CREATE SCHEMA ai_semantic;

CREATE FUNCTION ai_semantic.map(input text)
RETURNS text
AS 'MODULE_PATHNAME', 'semloom_marker_map'
LANGUAGE C
VOLATILE
PARALLEL UNSAFE;

COMMENT ON FUNCTION ai_semantic.map(text) IS
'Fail-closed SemMap marker; supported statements are lowered to a SemLoom CustomScan';

CREATE FUNCTION ai_semantic.filter(input text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'semloom_marker_filter'
LANGUAGE C
VOLATILE
PARALLEL UNSAFE;

COMMENT ON FUNCTION ai_semantic.filter(text) IS
'Fail-closed SemFilter marker; supported predicates are lowered to a SemLoom CustomScan';

CREATE FUNCTION ai_semantic.filter(input text, instruction text, options jsonb)
RETURNS boolean
AS 'MODULE_PATHNAME', 'semloom_marker_filter'
LANGUAGE C
VOLATILE
PARALLEL UNSAFE;

COMMENT ON FUNCTION ai_semantic.filter(text, text, jsonb) IS
'Fail-closed exact SemFilter marker; instruction and options become a planner-owned semantic plan';

CREATE FUNCTION ai_semantic.map(input text, instruction text, options jsonb)
RETURNS text
AS 'MODULE_PATHNAME', 'semloom_marker_map'
LANGUAGE C
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
CALLED ON NULL INPUT;

COMMENT ON FUNCTION ai_semantic.map(text, text, jsonb) IS
'Fail-closed generative SemMap marker; instruction and options become a planner-owned semantic plan';

-- A planner hook must be present before the first marker statement is planned.
LOAD 'MODULE_PATHNAME';
