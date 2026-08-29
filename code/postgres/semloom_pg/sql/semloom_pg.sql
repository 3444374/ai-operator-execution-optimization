\set VERBOSITY terse
\pset null '<NULL>'

CREATE EXTENSION semloom_pg;

CREATE TEMP TABLE semloom_documents (
    doc_id integer PRIMARY KEY,
    payload text NOT NULL
);

INSERT INTO semloom_documents VALUES
    (1, 'repeat'),
    (2, 'repeat'),
    (3, 'third');

EXPLAIN (COSTS OFF)
SELECT doc_id, ai_semantic.map(payload) AS completion
FROM semloom_documents
WHERE doc_id >= 2;

SELECT doc_id, ai_semantic.map(payload) AS completion
FROM semloom_documents
WHERE doc_id >= 2;

SELECT doc_id, ai_semantic.map(upper(payload)) AS completion
FROM semloom_documents
WHERE doc_id = 3;

SELECT ai_semantic.map(payload) AS completion
FROM semloom_documents
LIMIT 1;

SELECT ai_semantic.map(payload) AS completion
FROM semloom_documents
LIMIT 0;

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT ai_semantic.map(payload) AS completion
FROM semloom_documents
LIMIT 1;

SELECT upper(ai_semantic.map(payload))
FROM semloom_documents;

SELECT ai_semantic.map(payload)
FROM semloom_documents
ORDER BY doc_id;

SELECT ai_semantic.map(payload) AS completion
FROM semloom_documents
WHERE doc_id = 3;

CREATE TEMP TABLE semloom_nullable (payload text);
INSERT INTO semloom_nullable VALUES (NULL);
SELECT ai_semantic.map(payload) AS completion
FROM semloom_nullable;

CREATE TEMP TABLE semloom_filter_decisions (
    doc_id integer PRIMARY KEY,
    decision text
);
INSERT INTO semloom_filter_decisions VALUES
    (1, 'true'),
    (2, 'false'),
    (3, 'unknown'),
    (4, NULL),
    (5, 'true');

SELECT doc_id
FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision)
ORDER BY doc_id;

SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id >= 2 AND ai_semantic.filter(decision)
ORDER BY doc_id
LIMIT 1;

INSERT INTO semloom_filter_decisions VALUES (6, 'invalid');
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 6 AND ai_semantic.filter(decision);

SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 1 AND ai_semantic.filter(decision);

SELECT doc_id
FROM semloom_filter_decisions
WHERE NOT ai_semantic.filter(decision);

SELECT doc_id
FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision)
  AND ai_semantic.filter(decision || '');

SELECT ai_semantic.map(decision)
FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision);

CREATE TEMP TABLE semloom_sink (completion text);
BEGIN;
INSERT INTO semloom_sink
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE doc_id = 3;
TABLE semloom_sink;
ROLLBACK;
SELECT count(*) AS sink_rows FROM semloom_sink;

INSERT INTO semloom_sink
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE doc_id = 3;
TABLE semloom_sink;

INSERT INTO semloom_sink
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE doc_id = 3
RETURNING completion;

INSERT INTO semloom_sink
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE doc_id = 3
ON CONFLICT DO NOTHING;

SELECT count(*) AS sink_rows_after_errors FROM semloom_sink;

DROP EXTENSION semloom_pg CASCADE;
