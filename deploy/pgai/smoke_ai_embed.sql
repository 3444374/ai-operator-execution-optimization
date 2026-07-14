CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS ai CASCADE;

DROP TABLE IF EXISTS pgai_document_embeddings;
DROP TABLE IF EXISTS pgai_documents;

CREATE TABLE pgai_documents (
    doc_id INTEGER PRIMARY KEY,
    text TEXT NOT NULL
);

CREATE TABLE pgai_document_embeddings (
    doc_id INTEGER PRIMARY KEY REFERENCES pgai_documents(doc_id),
    embedding vector(384),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO pgai_documents (doc_id, text) VALUES
    (1, 'PostgreSQL triggers an AI embedding workload from SQL.'),
    (2, 'The external model service returns vectors for pgvector writeback.'),
    (3, 'Batching, queues, fan-in and writeback are the target system costs.');

INSERT INTO pgai_document_embeddings (doc_id, embedding)
SELECT doc_id, ai.ollama_embed('all-minilm', text)::vector(384)
FROM pgai_documents;

SELECT
    d.doc_id,
    left(d.text, 48) AS text_prefix,
    vector_dims(e.embedding) AS embedding_dims
FROM pgai_documents d
JOIN pgai_document_embeddings e USING (doc_id)
ORDER BY d.doc_id;
