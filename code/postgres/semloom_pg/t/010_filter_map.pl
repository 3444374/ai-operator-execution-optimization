use strict;
use warnings FATAL => 'all';
use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(encode_json decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('filter_map');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs(id integer, decision text, body text);
INSERT INTO inputs VALUES (1,'TRUE','hello'),(2,'FALSE','bad'),(3,'TRUE',NULL),(4,'UNKNOWN','bad');
CREATE TABLE outputs(id integer, body text, generated text);
CREATE TABLE ticks(value text);
CREATE FUNCTION tick(text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN INSERT INTO ticks VALUES ($1); RETURN $1; END $$;
});
my $socket = $node->host . '/filter-map.sock';
my $fixture = $node->basedir . '/filter-map.json';
open(my $base, '<', "$FindBin::RealBin/fixtures/filter_and.json") or die 'read fixture';
my $data = decode_json(do { local $/; <$base> });
close($base);
$data->{'e97d97db3b315860ef5a0b39258908945f74651b94b68f4d3c319800d680266d'} = {
    raw_output => 'generated', response_model_id => 'golden-map-v1',
    prompt_tokens => 17, output_tokens => 1, finish_reason => 'stop',
};
open(my $file, '>', $fixture) or die 'write fixture';
print $file encode_json($data);
close($file);
my $gateway_script = abs_path("$FindBin::RealBin/../../../scripts/services/run_execution_provider_gateway.py");
my ($gateway_out, $gateway_err) = ('', '');
my $gateway = IPC::Run::start(['python3', $gateway_script, '--socket', $socket,
    '--golden-fixture', $fixture, '--max-connections', '4', '--max-active-requests', '1'],
    '>', \$gateway_out, '2>', \$gateway_err);
END {
    if (defined $gateway && $gateway->pumpable) { $gateway->signal('TERM'); $gateway->finish; }
}
for (1..300) { last if -S $socket; select(undef, undef, undef, .01); }
ok(-S $socket, 'shared fixture gateway listens');
$node->append_conf('postgresql.conf', "semloom_pg.gateway_socket = '$socket'\n");
$node->reload;
my $filter_options = q|' {"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb|;
my $choice_options = q|' {"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|;
my $map_options = q|' {"model":"golden-map-v1","temperature":0,"max_tokens":128}'::jsonb|;
my $filter = "ai_semantic.filter(decision, 'Keep first.', $filter_options)";
sub map_expr { return "ai_semantic.map($_[0], 'Echo the input.', $map_options)"; }
my $map = map_expr('body');
my $query = "SELECT id, body, $map FROM ONLY inputs WHERE $filter";
sub sql { return $node->safe_psql('postgres', $_[0]); }
sub find_nodes {
    my ($plan, $name) = @_;
    my @found;
    push @found, $plan if ($plan->{'Custom Plan Provider'} // '') eq $name;
    for my $child (@{$plan->{'Plans'} // []}) { push @found, find_nodes($child, $name); }
    return @found;
}
sub plan_for { return decode_json(sql("EXPLAIN (ANALYZE, FORMAT JSON) $_[0]"))->[0]->{'Plan'}; }

is(sql($query), "1|hello|generated\n3||", 'Filter-to-Map preserves original input and SQL NULL');
my $plan = plan_for($query);
my ($map_node) = find_nodes($plan, 'SemLoom SemMap');
my ($filter_node) = find_nodes($plan, 'SemLoom SemFilter');
is($map_node->{'Input Binding'}, 'expression', 'Map owns final-stage input evaluation');
cmp_ok($map_node->{'Result Column'}, '>', 3, 'result has an independent internal column');
is($map_node->{'Model Calls'}, 1, 'Map requests only the nonnull survivor');
is($filter_node->{'Model Calls'}, 4, 'Filter independently evaluates all four inputs');
for my $predicate ("ai_semantic.filter(lower(decision))", "ai_semantic.filter(decision, 'Keep first.', $choice_options)")
{
    is(sql("SELECT id, body, $map FROM ONLY inputs WHERE $predicate"), "1|hello|generated\n3||",
       'recording and choice Filters use the same Map binding');
}
my $guarded = map_expr("CASE WHEN id=2 THEN (1/(id-id))::text ELSE body END");
is(sql("SELECT id, body, $guarded FROM ONLY inputs WHERE $filter"), "1|hello|generated\n3||",
   'discarded input does not evaluate the Map expression');
is(sql("SET semloom_pg.gateway_socket='/tmp/no-filter-map-provider.sock'; $query LIMIT 0"), '',
   'LIMIT zero needs no provider connection');
is(sql("$query LIMIT 1"), '1|hello|generated', 'LIMIT stops at the first survivor');
is(sql("SELECT id, $map FROM ONLY inputs WHERE id=2 AND $filter"), '', 'empty Filter output never evaluates Map');
for my $mode ('force_custom_plan', 'force_generic_plan')
{
    is(sql("SET plan_cache_mode=$mode; PREPARE composed AS $query; EXECUTE composed; EXECUTE composed;"),
       "1|hello|generated\n3||\n1|hello|generated\n3||", "$mode owns independent execution state");
}
sql("INSERT INTO outputs $query");
is(sql('SELECT * FROM outputs ORDER BY id'), "1|hello|generated\n3||", 'INSERT consumes the same bound results');
sql('TRUNCATE outputs;');
my ($status, $out, $err) = $node->psql('postgres', "INSERT INTO outputs SELECT id,body,$map FROM ONLY inputs WHERE ai_semantic.filter('TRUE','Keep first.',$filter_options);");
isnt($status, 0, 'missing Map fixture terminates INSERT');
is(sql('SELECT count(*) FROM outputs'), '0', 'failed INSERT writes no partial rows');
is(sql($query), "1|hello|generated\n3||", 'query recovers after Map failure');

sql(q{
CREATE ROLE composed_reader;
GRANT USAGE ON SCHEMA ai_semantic TO composed_reader;
GRANT SELECT ON inputs TO composed_reader;
CREATE TABLE secure_inputs AS TABLE inputs;
UPDATE secure_inputs SET decision='missing fixture',body='missing fixture' WHERE id=2;
ALTER TABLE secure_inputs ENABLE ROW LEVEL SECURITY;
CREATE POLICY composed_policy ON secure_inputs USING (id <> 2);
GRANT SELECT ON secure_inputs TO composed_reader;
});
(my $secure_query = $query) =~ s/ONLY inputs/ONLY secure_inputs/;
is(sql("SET ROLE composed_reader; $secure_query"), "1|hello|generated\n3||",
   'RLS-hidden input reaches neither provider');
my $snapshot = $node->background_psql('postgres');
$snapshot->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
is($snapshot->query_safe($query), "1|hello|generated\n3||", 'composition reads the initial snapshot');
sql("INSERT INTO inputs VALUES (5,'TRUE','hello')");
is($snapshot->query_safe($query), "1|hello|generated\n3||", 'both operators retain the transaction snapshot');
$snapshot->query_safe('COMMIT');
$snapshot->quit;
is(sql($query), "1|hello|generated\n3||\n5|hello|generated", 'new snapshot sees the committed input');
sql('DELETE FROM inputs WHERE id=5');
sql(q{
CREATE FUNCTION composed_wait(value text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN PERFORM pg_sleep(1); RETURN value; END $$;
});
my $waiting = map_expr('CASE WHEN id=3 THEN composed_wait(body) ELSE body END');
($status, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\nSET statement_timeout='300ms'; INSERT INTO outputs SELECT id,body,$waiting FROM ONLY inputs WHERE $filter");
like($err, qr/ERROR:  57014:/, 'cancellation interrupts late Map input evaluation');
is(sql('SELECT count(*) FROM outputs'), '0', 'cancelled composition rolls back earlier output');
is(sql($query), "1|hello|generated\n3||", 'both sessions recover after cancellation');

sql(q{
REVOKE EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) FROM PUBLIC;
CREATE FUNCTION capture_composed(statement text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN EXECUTE statement; RETURN 'unexpected success';
EXCEPTION WHEN OTHERS THEN RETURN SQLSTATE || '|' || SQLERRM;
END $$;
});
my $reader = $node->background_psql('postgres');
$reader->query_safe('SET ROLE composed_reader');
my $capture = sub {
    my $statement = $_[0];
    $statement =~ s/'/''/g;
    return $reader->query_safe("SELECT capture_composed('$statement')");
};
for my $statement ($query, "$query LIMIT 0", "EXPLAIN $query")
{
    is($capture->($statement), '42501|permission denied for function map',
       'bound result still checks Map EXECUTE before execution');
}
sql('GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO composed_reader');
for my $mode ('force_custom_plan', 'force_generic_plan')
{
    $reader->query_safe("SET plan_cache_mode=$mode; PREPARE composed_acl AS $query");
    is($reader->query_safe('EXECUTE composed_acl'), "1|hello|generated\n3||", "$mode executes authorized composition");
    sql('REVOKE EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) FROM composed_reader');
    is($capture->('EXECUTE composed_acl'), '42501|permission denied for function map',
       "$mode rechecks revoked marker permission");
    sql('GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO composed_reader');
    $reader->query_safe('DEALLOCATE composed_acl');
}
$reader->quit;
sql('GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO PUBLIC');
my $test_dir = abs_path("$FindBin::RealBin/plan_contract");
sql(qq{
CREATE FUNCTION composed_watch(oid,oid) RETURNS void
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_map_watch' LANGUAGE C STRICT;
CREATE FUNCTION composed_events() RETURNS text
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_map_events' LANGUAGE C;
CREATE FUNCTION composed_identity(text) RETURNS text LANGUAGE plpgsql VOLATILE
AS \$\$ BEGIN RETURN \$1; END \$\$;
});
my $hooks = $node->background_psql('postgres');
my $hook_map = map_expr('composed_identity(body)');
$hooks->query_safe(q{SELECT composed_watch('ai_semantic.map(text,text,jsonb)'::regprocedure,
    'composed_identity(text)'::regprocedure); SET plan_cache_mode=force_generic_plan;});
$hooks->query_safe("PREPARE composed_hook AS SELECT $hook_map FROM ONLY inputs WHERE $filter");
$hooks->query_safe('EXPLAIN EXECUTE composed_hook');
$hooks->query_safe('EXPLAIN EXECUTE composed_hook');
is($hooks->query_safe('SELECT composed_events()'), '2|2|1',
   'each cached initialization invokes marker and input hooks once and chains planning');
$hooks->quit;
sql('REVOKE EXECUTE ON FUNCTION composed_identity(text) FROM PUBLIC');
($status, $out, $err) = $node->psql('postgres',
    "\\set VERBOSITY verbose\nSET ROLE composed_reader; SELECT $hook_map FROM ONLY inputs WHERE $filter LIMIT 0");
like($err, qr/ERROR:  42501: permission denied for function composed_identity/,
     'input expression permission is checked even without a requested row');

sql("TRUNCATE inputs; INSERT INTO inputs SELECT i,'TRUE','hello' FROM generate_series(1,8) i;");
my $ticked = map_expr('tick(body)');
is(sql("SELECT tick(body),$ticked FROM ONLY inputs WHERE $filter OFFSET 3 LIMIT 1"), 'hello|generated',
   'OFFSET uses the independent generated result');
is(sql('SELECT count(*) FROM ticks'), 5, 'four native ordinary evaluations and one late Map input occurrence');
$plan = plan_for("$query OFFSET 3 LIMIT 1");
($map_node) = find_nodes($plan, 'SemLoom SemMap');
($filter_node) = find_nodes($plan, 'SemLoom SemFilter');
is($map_node->{'Model Calls'}, 1, 'OFFSET skips Map requests');
is($filter_node->{'Model Calls'}, 4, 'Filter finds the fourth survivor without prefetch');
for my $unsupported (
    "$query AND $filter",
    "SELECT $map FROM ONLY inputs WHERE $filter OR id=1",
    'SELECT ' . map_expr($map) . " FROM ONLY inputs WHERE $filter")
{
    ($status, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\nEXPLAIN $unsupported");
    isnt($status, 0, 'unsupported composition remains rejected');
    like($err, qr/ERROR:  0A000:/, 'unsupported composition is a planning error');
}
$node->stop;
$gateway->signal('TERM');
$gateway->finish;
ok(!-e $socket, 'fixture gateway removes its socket');
done_testing();
