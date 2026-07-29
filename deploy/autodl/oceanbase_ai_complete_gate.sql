-- OceanBase AI_COMPLETE capability gate.
-- Section 1 is read-only. Run Section 2 only after replacing every
-- BASELINE_* value with the intended disposable gate registration.

-- Section 1: version and capability discovery.
SELECT VERSION() AS oceanbase_version;

SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE
FROM information_schema.ROUTINES
WHERE UPPER(ROUTINE_NAME) IN (
    'CREATE_AI_MODEL',
    'CREATE_AI_MODEL_ENDPOINT'
)
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;

SELECT TABLE_SCHEMA, TABLE_NAME
FROM information_schema.TABLES
WHERE UPPER(TABLE_NAME) IN (
    'DBA_OB_AI_MODELS',
    'DBA_OB_AI_MODEL_ENDPOINTS'
)
ORDER BY TABLE_SCHEMA, TABLE_NAME;

SELECT
    ENDPOINT_NAME,
    AI_MODEL_NAME,
    URL,
    PROVIDER,
    REQUEST_MODEL_NAME
FROM oceanbase.DBA_OB_AI_MODEL_ENDPOINTS
ORDER BY ENDPOINT_NAME;

-- Section 2: explicit one-row gate. Do not run with placeholder values.
-- This section intentionally does not drop or overwrite registrations.
SET @baseline_model_key = 'BASELINE_MODEL_KEY';
SET @baseline_endpoint_name = 'BASELINE_ENDPOINT_NAME';
SET @baseline_model_name = 'BASELINE_SERVED_MODEL_NAME';
SET @baseline_endpoint_url =
    'http://127.0.0.1:8000/v1/chat/completions';
SET @baseline_access_key = 'not-needed';

CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(
    @baseline_model_key,
    JSON_OBJECT(
        'type', 'completion',
        'model_name', @baseline_model_name
    )
);

CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(
    @baseline_endpoint_name,
    JSON_OBJECT(
        'ai_model_name', @baseline_model_key,
        'url', @baseline_endpoint_url,
        'access_key', @baseline_access_key,
        'provider', 'openai'
    )
);

SELECT AI_COMPLETE(
    @baseline_model_key,
    'Reply with exactly: OCEANBASE_AI_COMPLETE_OK',
    JSON_OBJECT('temperature', 0.0, 'max_tokens', 16)
) AS gate_output;
