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

LOAD 'MODULE_PATHNAME';
