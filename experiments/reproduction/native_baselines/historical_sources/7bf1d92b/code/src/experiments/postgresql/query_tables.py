"""Install one immutable, bounded raw input relation and verify its identity.

Setup is outside query timing. No answer/label column is accepted, no existing
relation is replaced, and a failed setup rolls back the caller-owned new table.
"""
import hashlib
import json


def install_input_table(connection, inputs, rows):
    from psycopg import sql
    if not connection.autocommit or connection.closed or int(connection.info.transaction_status) != 0:
        raise ValueError('table setup requires a dedicated idle autocommit connection')
    table = sql.Identifier(inputs.table)
    digest = hashlib.sha256()
    count = 0
    previous_position = -1
    columns = sql.SQL(',').join(sql.SQL('{} text NOT NULL').format(sql.Identifier(c)) for c in inputs.columns)
    byte_total = sql.SQL(' + ').join(sql.SQL('octet_length({})').format(sql.Identifier(c))
                                   for c in ('row_id', *inputs.columns))
    with connection.transaction():
        connection.execute(sql.SQL('''CREATE TABLE {} (
            source_position bigint PRIMARY KEY CHECK(source_position>=0), row_id text NOT NULL UNIQUE,
            {}, CHECK({}<={}))''').format(table, columns, byte_total, sql.Literal(inputs.max_source_bytes)))
        with connection.cursor().copy(sql.SQL('COPY {} FROM STDIN').format(table)) as copy:
            for row in rows:
                row=inputs.normalize(row)
                inputs.convert(row)
                if row[0] <= previous_position:
                    raise ValueError('source positions must be unique and increasing')
                previous_position = row[0]
                count += 1
                if count > inputs.max_rows:
                    raise ValueError('too many source rows for the declared relation')
                copy.write_row(row)
                digest.update((json.dumps(list(row), ensure_ascii=False, separators=(',', ':'))+'\n').encode())
        # RETURN NOTHING rules do not reliably reject COPY/TRUNCATE; triggers do.
        function = sql.Identifier('query_immutable_'+hashlib.sha256(inputs.table.encode()).hexdigest()[:20])
        connection.execute(sql.SQL("CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'immutable benchmark input'; END$$").format(function))
        connection.execute(sql.SQL('CREATE TRIGGER immutable_rows BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {} FOR EACH STATEMENT EXECUTE FUNCTION {}()').format(table, function))
        connection.execute(sql.SQL('ANALYZE {}').format(table))
        actual = connection.execute(sql.SQL('SELECT count(*) FROM {}').format(table)).fetchone()[0]
        if actual != count:
            raise ValueError('source import row count differs')
        readback = hashlib.sha256()
        statement = sql.SQL('SELECT source_position,row_id,{} FROM {} ORDER BY source_position').format(
            sql.SQL(',').join(map(sql.Identifier, inputs.columns)), table)
        with connection.cursor() as cursor:
            for row in cursor.stream(statement):
                readback.update((json.dumps(list(row), ensure_ascii=False, separators=(',', ':'))+'\n').encode())
        if readback.digest() != digest.digest():
            raise ValueError('raw input roundtrip digest differs')
    return dict(rows=count, source_sha256=digest.hexdigest(), columns=['source_position','row_id',*inputs.columns],
                table=inputs.table, max_source_bytes=inputs.max_source_bytes, immutable=True,
                identity_order='input iteration; callers must provide unique increasing source_position')


def map_statement(inputs, plan):
    """PG owns the Map call and any raw-column message construction."""
    from psycopg import sql
    if inputs.kind == 'movie':
        value = sql.Identifier('review_text')
    else:
        value = sql.SQL("{} || context || {} || question || {}").format(
            sql.Literal('Context:\n'), sql.Literal('\n\nQuestion:\n'), sql.Literal('\n\nAnswer:\n'))
    statement = sql.SQL('SELECT row_id,ai_semantic.map({},{},{}::jsonb) FROM ONLY {}').format(
        value, sql.Literal(plan.instruction),
        sql.Literal(json.dumps(dict(model=plan.model_id, temperature=0, max_tokens=plan.max_tokens))),
        sql.Identifier(inputs.table))
    if inputs.movie_id is not None:
        statement += sql.SQL(' WHERE movie_id={}').format(sql.Literal(inputs.movie_id))
    return statement
