CREATE FUNCTION ai_semantic.embed(input bytea, options jsonb)
RETURNS real[]
AS 'MODULE_PATHNAME', 'semloom_marker_image'
LANGUAGE C
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
CALLED ON NULL INPUT;

COMMENT ON FUNCTION ai_semantic.embed(bytea, jsonb) IS
'Typed image embedding marker; fixed CLIP semantics and query lifecycle belong to PostgreSQL';
