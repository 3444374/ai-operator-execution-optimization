use strict;
use warnings FATAL => 'all';
use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('filter_and');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\nstatement_timeout = '5s'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs(id integer, a text, b text);
INSERT INTO inputs VALUES
 (1,'TRUE','TRUE'), (2,'FALSE','TRUE'), (3,'TRUE','FALSE'), (4,'TRUE','UNKNOWN'),
 (5,NULL,'TRUE'), (6,'UNKNOWN','TRUE'), (7,'TRUE',NULL), (8,'TRUE','TRUE'),
 (9,'','TRUE'), (10,U&'unicode \03BB','TRUE');
CREATE TABLE outputs(id integer);
});
my $options = q|' {"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb|;
my $choice = q|' {"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|;
my $first = "ai_semantic.filter(a, 'Keep first.', $options)";
my $second = "ai_semantic.filter(b, 'Keep second.', $options)";
my $query = "SELECT id FROM inputs WHERE $first AND $second";
my $socket = $node->host . '/and.sock';
my $gateway_script = abs_path("$FindBin::RealBin/../../../scripts/services/run_execution_provider_gateway.py");
my $fixture = abs_path("$FindBin::RealBin/fixtures/filter_and.json");
my ($gateway_out, $gateway_err) = ('', '');
my $gateway = IPC::Run::start(['python3', $gateway_script, '--socket', $socket,
 '--golden-fixture', $fixture, '--max-connections', '4', '--max-active-requests', '1'],
 '>', \$gateway_out, '2>', \$gateway_err);
END {
 if (defined $gateway && $gateway->pumpable) {
  $gateway->signal('TERM');
  $gateway->finish;
 }
}
for (1..300) { last if -S $socket; select(undef, undef, undef, .01); }
ok(-S $socket, 'one shared gateway starts');
$node->append_conf('postgresql.conf', "semloom_pg.gateway_socket = '$socket'\n");
$node->reload;

sub sql {
 my ($text) = @_;
 return $node->safe_psql('postgres', $text);
}
sub filter_nodes {
 my ($plan) = @_;
 my @nodes;
 push @nodes, $plan if ($plan->{'Custom Plan Provider'} // '') eq 'SemLoom SemFilter';
 for my $child (@{$plan->{'Plans'} // []}) { push @nodes, filter_nodes($child); }
 return @nodes;
}
sub plan_for {
 my ($text) = @_;
 return decode_json(sql($text))->[0]->{'Plan'};
}

my @filters = filter_nodes(plan_for("EXPLAIN (FORMAT JSON) $query"));
is(scalar @filters, 2, 'AND has two visible Filter nodes');
my @choice_filters = filter_nodes(plan_for("EXPLAIN (FORMAT JSON) SELECT id FROM inputs WHERE ai_semantic.filter(a, 'Keep first.', $choice) AND ai_semantic.filter(b, 'Keep second.', $choice)"));
isnt($choice_filters[0]->{'Semantic Spec Digest'}, $choice_filters[1]->{'Semantic Spec Digest'},
 'different instructions retain independent specs');
is($filters[0]->{'AI Cost Calibration Reason'}, 'upstream-semantic-filter',
 'downstream subset does not borrow base-input calibration');
is(sql("$query ORDER BY id"), "1\n8\n9\n10", 'AND preserves duplicates, Unicode, empty and NULL/UNKNOWN semantics');
@filters = filter_nodes(plan_for("EXPLAIN (ANALYZE, FORMAT JSON) $query"));
is(scalar @filters, 2, 'both actual Filter nodes are visible');
is($filters[1]->{'Model Calls'}, 9, 'first Filter skips SQL NULL');
is($filters[0]->{'Model Calls'}, 6, 'second Filter sees only upstream survivors and skips its NULL');
is($filters[1]->{'Emitted Rows'}, 7, 'upstream emitted count is independent');
is($filters[0]->{'Emitted Rows'}, 4, 'downstream emitted count is independent');
is(sql("$query LIMIT 1"), '1', 'LIMIT stops the composed output');
is(sql("$query LIMIT 0"), '', 'LIMIT zero returns nothing');
is(sql("SELECT id FROM inputs WHERE id < 0 AND $first AND $second"), '', 'empty ordinary child needs no provider tasks');
is(sql("SELECT id FROM inputs WHERE a IS NULL AND $first AND $second"), '', 'NULL-only upstream produces no downstream input');

is(sql("SELECT id FROM inputs WHERE $first AND ai_semantic.filter(CASE WHEN id=2 THEN (1/(id-id))::text ELSE b END, 'Keep second.', $options) ORDER BY id"),
 "1\n8\n9\n10", 'downstream expression is not evaluated for upstream-discarded rows');
is(sql("SELECT id FROM inputs WHERE $first AND ai_semantic.filter(a, 'Keep second.', $options) ORDER BY id"),
 "1\n3\n4\n7\n8\n9\n10", 'same input has two independent calls');
@filters = filter_nodes(plan_for("EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM inputs WHERE $first AND $first"));
is(scalar @filters, 2, 'identical expressions retain two call instances');
is($filters[1]->{'Model Calls'}, 9, 'identical first occurrence calls once per nonnull input');
is($filters[0]->{'Model Calls'}, 7, 'identical second occurrence executes on survivors');

sql(q{
CREATE SEQUENCE pair_seq;
CREATE FUNCTION pair_tick() RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN PERFORM nextval('pair_seq'); RETURN 'TRUE'; END $$;
});
is(sql("SELECT id FROM inputs WHERE id=1 AND ai_semantic.filter(pair_tick(), 'Keep first.', $options) AND ai_semantic.filter(pair_tick(), 'Keep second.', $options)"),
 '1', 'both volatile-input Filter calls execute');
is(sql('SELECT last_value FROM pair_seq'), '2', 'each Filter evaluates its own volatile input');

is(sql("SELECT b, id+100 FROM inputs WHERE $first AND $second ORDER BY id"),
 "TRUE|101\nTRUE|108\nTRUE|109\nTRUE|110", 'projection and input binding remain distinct');
sql("INSERT INTO outputs $query");
is(sql('SELECT id FROM outputs ORDER BY id'), "1\n8\n9\n10", 'INSERT writes the composed result');
is(sql("SET plan_cache_mode=force_generic_plan; PREPARE pair(integer) AS $query AND id >= \$1; EXECUTE pair(8); EXECUTE pair(10); DEALLOCATE pair"),
 "8\n9\n10\n10", 'generic prepared plans retain both operators and parameters');
is(sql("SELECT id FROM inputs WHERE $first AND ai_semantic.filter(b, 'Keep second.', $choice) ORDER BY id"),
 "1\n8\n9\n10", 'v3 and v4 coexist at one gateway');
is(sql("SELECT id FROM inputs WHERE id<=8 AND ai_semantic.filter(lower(a)) AND $second ORDER BY id"),
 "1\n8", 'recording and exact Filter coexist');

for my $bad (
 "SELECT id FROM inputs WHERE $first OR $second",
 "SELECT id FROM inputs WHERE NOT ($first AND $second)",
 "SELECT id FROM inputs WHERE $first AND $second AND ai_semantic.filter(a)",
 "SELECT ai_semantic.map(a) FROM inputs WHERE $first AND $second")
{
 my ($status, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\n$bad");
 isnt($status, 0, 'unsupported composition fails closed');
 like($err, qr/ERROR:  0A000:/, 'unsupported shape has stable SQLSTATE');
}
my ($status, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\nSELECT id FROM inputs WHERE $first AND ai_semantic.filter(b, '', $options)");
like($err, qr/ERROR:  22023:/, 'second call arguments are validated before execution');
($status, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\nBEGIN; INSERT INTO outputs SELECT id FROM inputs WHERE $first AND ai_semantic.filter('missing fixture', 'Keep second.', $options); COMMIT;");
isnt($status, 0, 'downstream provider error aborts INSERT');
is(sql('SELECT count(*) FROM outputs'), '4', 'failed INSERT leaves no partial rows');
is(sql("$query ORDER BY id"), "1\n8\n9\n10", 'another query recovers through the same gateway');

sql('CREATE ROLE pair_reader; GRANT USAGE ON SCHEMA ai_semantic TO pair_reader; GRANT SELECT ON inputs TO pair_reader;');
is(sql("SET ROLE pair_reader; $query ORDER BY id"), "1\n8\n9\n10", 'ordinary source permissions allow both operators');
sql(q{
CREATE TABLE secure_inputs AS TABLE inputs;
UPDATE secure_inputs SET a='missing fixture' WHERE id=2;
ALTER TABLE secure_inputs ENABLE ROW LEVEL SECURITY;
CREATE POLICY pair_policy ON secure_inputs USING (id <> 2);
GRANT SELECT ON secure_inputs TO pair_reader;
});
(my $secure_query = $query) =~ s/FROM inputs/FROM secure_inputs/;
is(sql("SET ROLE pair_reader; $secure_query ORDER BY id"), "1\n8\n9\n10",
 'RLS-hidden input reaches neither Filter');
my $snapshot = $node->background_psql('postgres');
$snapshot->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ;');
is($snapshot->query("$query ORDER BY id"), "1\n8\n9\n10", 'both nodes share the initial snapshot');
sql("INSERT INTO inputs VALUES (11,'TRUE','TRUE')");
is($snapshot->query("$query ORDER BY id"), "1\n8\n9\n10", 'both nodes retain the transaction snapshot');
$snapshot->query_safe('COMMIT;');
$snapshot->quit;
is(sql("$query ORDER BY id"), "1\n8\n9\n10\n11", 'a new snapshot sees the committed row');
sql('DELETE FROM inputs WHERE id=11');
sql(q{
CREATE FUNCTION pair_wait(value text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN PERFORM pg_sleep(1); RETURN value; END $$;
});
($status, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\nSET statement_timeout='300ms'; INSERT INTO outputs SELECT id FROM inputs WHERE $first AND ai_semantic.filter(CASE WHEN id=8 THEN pair_wait(b) ELSE b END, 'Keep second.', $options)");
like($err, qr/ERROR:  57014:/, 'cancellation interrupts the composed INSERT');
is(sql('SELECT count(*) FROM outputs'), '4', 'cancelled INSERT leaves no partial write');
is(sql("$query ORDER BY id"), "1\n8\n9\n10", 'shared gateway serves a new composed query after cancellation');

sql('REVOKE EXECUTE ON FUNCTION ai_semantic.filter(text,text,jsonb) FROM PUBLIC;');
($status, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\nSET ROLE pair_reader; $query");
like($err, qr/ERROR:  42501:/, 'function permission is checked for the composed path');
sql('GRANT EXECUTE ON FUNCTION ai_semantic.filter(text,text,jsonb) TO PUBLIC;');

$gateway->signal('TERM');
$gateway->finish;
ok($gateway->result(0) == 0, 'shared gateway exits cleanly');
is($gateway_err, '', 'gateway has no unexpected diagnostics');
ok(!-e $socket, 'gateway removes its listener');
$node->stop;
done_testing();
