\set VERBOSITY terse

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

SELECT upper(ai_semantic.map(payload))
FROM semloom_documents;

SELECT ai_semantic.map(payload)
FROM semloom_documents
ORDER BY doc_id;

DROP EXTENSION semloom_pg CASCADE;
