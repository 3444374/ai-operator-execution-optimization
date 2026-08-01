-- Migrate image row identity from global doc_id to workload-scoped doc_id.
-- Idempotent for the intended legacy/current schemas; fail closed otherwise.

BEGIN;
LOCK TABLE public.image_documents IN ACCESS EXCLUSIVE MODE;

DO $migration$
DECLARE
    current_constraint text;
    current_columns text[];
BEGIN
    SELECT constraint_name, columns
      INTO current_constraint, current_columns
      FROM (
        SELECT c.conname AS constraint_name,
               array_agg(a.attname::text ORDER BY key_column.ordinality) AS columns
          FROM pg_constraint AS c
          CROSS JOIN LATERAL unnest(c.conkey)
            WITH ORDINALITY AS key_column(attnum, ordinality)
          JOIN pg_attribute AS a
            ON a.attrelid = c.conrelid AND a.attnum = key_column.attnum
         WHERE c.conrelid = 'public.image_documents'::regclass
           AND c.contype = 'p'
         GROUP BY c.conname
      ) AS primary_key;

    IF current_columns = ARRAY['workload_name', 'doc_id']::text[] THEN
        RAISE NOTICE 'image_documents already uses workload-scoped identity';
        RETURN;
    END IF;
    IF current_columns IS DISTINCT FROM ARRAY['doc_id']::text[] THEN
        RAISE EXCEPTION
          'unexpected image_documents primary key: %, refusing migration',
          current_columns;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM public.image_documents
         GROUP BY workload_name, doc_id
        HAVING count(*) > 1
         LIMIT 1
    ) THEN
        RAISE EXCEPTION 'duplicate (workload_name, doc_id) rows block migration';
    END IF;

    EXECUTE format(
        'ALTER TABLE public.image_documents DROP CONSTRAINT %I',
        current_constraint
    );
    ALTER TABLE public.image_documents
      ADD CONSTRAINT image_documents_pkey PRIMARY KEY (workload_name, doc_id);
END
$migration$;

COMMIT;
