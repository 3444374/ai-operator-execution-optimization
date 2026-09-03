use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(encode_json decode_json);
use IO::Select;
use IO::Socket::UNIX;
use Socket qw(SOCK_STREAM);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $node = PostgreSQL::Test::Cluster->new('map_execution');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE map_rows(id integer, body text);
INSERT INTO map_rows VALUES (1, 'hello');
});
my $socket = $node->host . '/map.sock';
my $fixture = $node->basedir . '/map.json';
open(my $file, '>', $fixture) or die 'cannot create Map fixture';
print $file encode_json({
    'e97d97db3b315860ef5a0b39258908945f74651b94b68f4d3c319800d680266d' => {
        raw_output => 'hello', response_model_id => 'golden-map-v1',
        prompt_tokens => 17, output_tokens => 1, finish_reason => 'stop',
    },
    '04e2e3e0c1a42742676ad000b8078ee01818d8bf78351ebed5b570787c477df8' => {
        raw_output => '', response_model_id => 'golden-map-v1',
        prompt_tokens => 8, output_tokens => 0, finish_reason => 'stop',
    },
    'ea61042f35816954d5d477f907e3667dbfb38d0dae9ac1f589e40838aeed4b32' => {
        raw_output => "\x{7532}\n\"\x{4e59}\"\\\x{4e19}\t{}", response_model_id => 'golden-map-v1',
        prompt_tokens => 19, output_tokens => 9, finish_reason => 'stop',
    },
});
close($file);
my $gateway_script = abs_path("$FindBin::RealBin/../gateway/recording_gateway.py");
my $options = q|' {"model":"golden-map-v1","temperature":0,"max_tokens":128}'::jsonb|;

sub run_query
{
    my ($sql, $script, @arguments) = @_;
    my ($output, $error) = ('', '');
    my $process = IPC::Run::start(['python3', $script, '--socket', $socket, @arguments],
        '>', \$output, '2>', \$error, IPC::Run::timeout(15));
    for (1 .. 200) { last if -S $socket; sleep(0.01); }
    ok(-S $socket, 'Map peer listens') or diag($error);
    my @result = $node->psql('postgres',
        "\\set VERBOSITY verbose\nSET statement_timeout='5s'; SET semloom_pg.gateway_socket='$socket';\n$sql");
    my $finished = eval { $process->finish };
    if ($@) { $process->kill_kill; }
    ok($finished, 'Map peer exits after the session') or diag($error);
    ok(!-e $socket, 'Map peer removes its listener');
    return @result;
}
sub golden_query
{
    my ($sql, @arguments) = @_;
    my $multiple = grep { $_ eq '--test-max-sessions' } @arguments;
    return run_query($sql, $gateway_script, ($multiple ? () : ('--once')),
        '--golden-fixture', $fixture, @arguments);
}
my $peer_script = abs_path("$FindBin::RealBin/fixtures/map_wire_peer.py");
my $map = "ai_semantic.map(body, 'Echo the input.', $options)";
my $query = "SELECT id, $map FROM ONLY map_rows";
my ($status, $stdout, $stderr) = golden_query($query);
is($status, 0, 'generative Map executes through PostgreSQL and golden wire v5') or diag($stderr);
is($stdout, '1|hello', 'the independent ASCII vector returns generated text');
for my $projection (
    ["ai_semantic.map('hello', 'Echo the input.', $options)", 'generated'],
    ["body, $map", 'hello|generated'],
    ["$map, body", 'generated|hello'],
    ["body || '!', ai_semantic.map(body || '!', 'Echo the input.', $options)", 'hello!|generated'])
{
    ($status, $stdout, $stderr) = run_query("SELECT $projection->[0] FROM ONLY map_rows", $peer_script, '--output', 'generated');
    is($status, 0, 'distinct Map output binding executes') or diag($stderr);
    is($stdout, $projection->[1], 'generated output cannot alias an equal ordinary input expression');
}
for my $projection (
    ["ai_semantic.map('hello')", 'recorded:hello'],
    ['body, ai_semantic.map(body)', 'hello|recorded:hello'],
    ['ai_semantic.map(body), body', 'recorded:hello|hello'])
{
    is($node->safe_psql('postgres', "SELECT $projection->[0] FROM ONLY map_rows"), $projection->[1],
       'recording Map also keeps a distinct generated output position');
}
$node->safe_psql('postgres', q{INSERT INTO map_rows VALUES (2,NULL),(3,''),(4,'hello');});
($status, $stdout, $stderr) = golden_query($query);
is($status, 0, 'Map preserves cardinality across NULL, empty input and duplicates') or diag($stderr);
is($stdout, "1|hello\n2|\n3|\n4|hello", 'generated text stays associated with its input row');
($status, $stdout, $stderr) = golden_query("EXPLAIN (ANALYZE, FORMAT JSON) $query");
is($status, 0, 'Map EXPLAIN ANALYZE executes') or diag($stderr);
my $plan = decode_json($stdout)->[0]->{'Plan'};
is($plan->{'Provider'}, 'uds-golden', 'generated Map identifies the semantic adapter');
is($plan->{'Model Calls'}, 3, 'NULL consumes no model task');
is($plan->{'Prompt Tokens'}, 42, 'actual prompt usage includes zero/empty completion metadata');
is($plan->{'Output Tokens'}, 2, 'actual output usage preserves explicit zero');
is($plan->{'Accepted Rows'}, 3, 'sequence progresses only for non-NULL tasks');
is($plan->{'Emitted Rows'}, 3, 'successful task counter excludes local NULL propagation');
($status, $stdout, $stderr) = golden_query("$query LIMIT 1 OFFSET 2");
is($status, 0, 'LIMIT and OFFSET close a partially consumed Map session') or diag($stderr);
is($stdout, '3|', 'OFFSET counts the propagated NULL row');

$node->safe_psql('postgres', q{CREATE TABLE map_sink(id integer, generated text);});
($status, $stdout, $stderr) = golden_query("INSERT INTO map_sink $query; SELECT id, generated, generated IS NULL FROM map_sink ORDER BY id;");
is($status, 0, 'generated Map INSERT uses the same executed path') or diag($stderr);
is($stdout, "1|hello|f\n2||t\n3||f\n4|hello|f", 'INSERT distinguishes SQL NULL from empty generated text');
($status, $stdout, $stderr) = golden_query("BEGIN; INSERT INTO map_sink $query; ROLLBACK; SELECT count(*) FROM map_sink;");
is($status, 0, 'generated Map write participates in rollback') or diag($stderr);
is($stdout, '4', 'rollback discards newly generated rows');

$node->safe_psql('postgres', q{INSERT INTO map_rows VALUES(5,U&'\7532\000A"\4E59"' || chr(92) || U&'\4E19\0009{}');});
is($node->safe_psql('postgres', "SELECT encode(convert_to(body,'UTF8'),'hex') FROM map_rows WHERE id=5"),
   'e794b20a22e4b999225ce4b899097b7d', 'SQL fixture bytes independently match the Unicode input vector');
my $unicode = "INSERT INTO map_sink SELECT id, ai_semantic.map(body, U&'\\539F\\6837\\8FD4\\56DE\\8F93\\5165\\3002', $options) FROM ONLY map_rows WHERE id=5; SELECT encode(convert_to(generated,'UTF8'),'hex') FROM map_sink WHERE id=5;";
($status, $stdout, $stderr) = golden_query($unicode);
is($status, 0, 'Unicode instruction/input cross C and Python without canonicalization drift') or diag($stderr);
is($stdout, 'e794b20a22e4b999225ce4b899097b7d', 'multibyte raw text and control characters are preserved');

for my $raw ('', ' TRUE ', 'FALSE', 'UNKNOWN', 'NULL', "line one\nline two\t{}")
{
    ($status, $stdout, $stderr) = run_query("TRUNCATE map_sink; INSERT INTO map_sink $query WHERE id=1; SELECT encode(convert_to(generated,'UTF8'),'hex'), generated IS NULL FROM map_sink;",
        $peer_script, '--output', $raw);
    is($status, 0, 'Map accepts arbitrary valid generated text') or diag($stderr);
    is($stdout, unpack('H*', $raw) . '|f', 'Map does not trim or reinterpret text as Filter/SQL labels');
}

for my $mode ('force_custom_plan', 'force_generic_plan')
{
    ($status, $stdout, $stderr) = golden_query("SET plan_cache_mode=$mode; PREPARE mapped(integer) AS $query WHERE id=\$1; EXECUTE mapped(1); ALTER TABLE map_rows ADD COLUMN extra_$mode integer; EXECUTE mapped(1);", '--test-max-sessions', '2');
    is($status, 0, "$mode keeps Map execution after relation invalidation") or diag($stderr);
    is($stdout, "1|hello\n1|hello", 'prepared binding returns the same generated values');
}

$node->safe_psql('postgres', q{
CREATE SEQUENCE map_input_calls;
CREATE FUNCTION counted_map_input(value text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN PERFORM nextval('map_input_calls'); RETURN value; END $$;
CREATE FUNCTION checked_map_input(value text, row_id integer) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN IF row_id=4 THEN PERFORM 1/0; END IF; RETURN value; END $$;
});
for my $limit ('', 'LIMIT 2', 'LIMIT 1 OFFSET 2')
{
    my $ordinary = $node->safe_psql('postgres', "ALTER SEQUENCE map_input_calls RESTART WITH 1; SELECT counted_map_input(body) FROM ONLY map_rows WHERE id<=4 $limit; SELECT last_value FROM map_input_calls;");
    my @lines = split /\n/, $ordinary;
    my $expected_calls = $lines[-1];
    ($status, $stdout, $stderr) = golden_query("ALTER SEQUENCE map_input_calls RESTART WITH 1; SELECT ai_semantic.map(counted_map_input(body),'Echo the input.',$options) FROM ONLY map_rows WHERE id<=4 $limit; SELECT last_value FROM map_input_calls;");
    is($status, 0, 'VOLATILE Map input executes with ordinary child evaluation') or diag($stderr);
    @lines = split /\n/, $stdout;
    is($lines[-1], $expected_calls, 'Map does not add input expression evaluations before/after LIMIT');
}
my $child_query = "SELECT id, ai_semantic.map(checked_map_input(body,id),'Echo the input.',$options) FROM ONLY map_rows WHERE id IN (1,4)";
($status, $stdout, $stderr) = golden_query("TRUNCATE map_sink; \\set ON_ERROR_STOP 0\nBEGIN; SAVEPOINT before_child; INSERT INTO map_sink $child_query; ROLLBACK TO SAVEPOINT before_child; $query WHERE id=1; ROLLBACK; SELECT count(*) FROM map_sink;", '--test-max-sessions', '2');
like($stderr, qr/ERROR:  22012: division by zero\n/, 'later child expression error retains native SQLSTATE');
is($stdout, "1|hello\n0", 'later child failure closes the session and leaves no partial INSERT rows');

my @faults = (
    (map { ["open-$_", '08P01', 'SemLoom provider open response does not match wire v5'] }
        qw(version fractional max_input_bytes max_output_bytes max_frame_bytes max_inflight_tasks semantic_spec_digest physical_algorithm_digest provider_execution_digest)),
    (map { ["completion-$_", '08P01', 'SemLoom provider completion does not match wire v5 task identity'] }
        qw(version fractional sequence semantic_spec_digest physical_algorithm_digest provider_execution_digest semantic_payload_digest completion_evidence_digest model usage-budget usage-total-overflow finish-empty finish-long over-output-evidence)),
    (map { ["completion-$_", '08P01', 'SemLoom provider response has an invalid uint64 field'] }
        qw(usage-overflow usage-leading-zero)),
    (map { ["completion-$_", '08P01', 'SemLoom provider response has an invalid text field'] }
        qw(sequence-number usage-number null)),
    (map { [$_, '08P01', 'SemLoom provider returned an unexpected message'] }
        qw(open-extra open-missing completion-extra completion-missing)),
    (map { [$_, '08P01', 'SemLoom provider returned invalid JSON'] }
        qw(open-duplicate completion-duplicate completion-escaped-nul completion-raw-nul completion-utf8 completion-utf8-over-output)),
    (map { ["completion-$_", '22000', 'SemMap model completion must finish with stop'] }
        qw(finish-length finish-tool finish-space)),
    (map { [$_, '54000', 'SemLoom provider message exceeds its configured limit'] }
        qw(completion-over-output completion-over-output-finish error-valid)),
    (map { ["error-$_", '08P01', 'SemLoom provider returned an invalid wire v5 error frame'] }
        qw(extra missing version sequence code)),
);
for my $case (@faults)
{
    my ($fault, $state, $message) = @$case;
    ($status, $stdout, $stderr) = run_query("$query WHERE id=1", $peer_script, '--fault', $fault);
    isnt($status, 0, "$fault terminates the Map query");
    like($stderr, qr/ERROR:  \Q$state\E: \Q$message\E\n/, "$fault preserves its exact redacted error");
    unlike($stderr, qr/private-provider-text|wrong-model/, 'provider values do not appear in errors');
}

($status, $stdout, $stderr) = run_query("TRUNCATE map_sink; INSERT INTO map_sink $query WHERE id=1; SELECT octet_length(generated) FROM map_sink;",
    $peer_script, '--output-length', '65536');
is($status, 0, 'the full 65536-byte completion is accepted') or diag($stderr);
is($stdout, '65536', 'the output boundary is not truncated');
($status, $stdout, $stderr) = run_query("$query WHERE id=1", $peer_script, '--output-length', '65537');
isnt($status, 0, 'oversized fixture completion is rejected by the canonical session');
like($stderr, qr/ERROR:  54000: SemLoom provider message exceeds its configured limit\n/, 'v5 OUTPUT_TOO_LARGE reaches the PG length error');

my $sentinel = IO::Socket::UNIX->new(Type => SOCK_STREAM, Local => $socket, Listen => 8)
    or die 'cannot create no-task sentinel';
for my $sql ("EXPLAIN $query", "$query LIMIT 0", "$query WHERE false", "$query WHERE id=2")
{
    ($status, $stdout, $stderr) = $node->psql('postgres', "SET semloom_pg.gateway_socket='$socket'; $sql");
    is($status, 0, 'no-task Map succeeds with a provider that never responds') or diag($stderr);
    ok(!IO::Select->new($sentinel)->can_read(0), 'no-task Map makes zero provider connections');
}
($status, $stdout, $stderr) = $node->psql('postgres', "\\set VERBOSITY verbose\nSET semloom_pg.gateway_socket='$socket'; SELECT ai_semantic.map(checked_map_input(body,id),'Echo the input.',$options) FROM ONLY map_rows WHERE id=4");
like($stderr, qr/ERROR:  22012: division by zero\n/, 'first child expression failure retains native SQLSTATE');
ok(!IO::Select->new($sentinel)->can_read(0), 'first child error precedes provider open');
($status, $stdout, $stderr) = $node->psql('postgres', "\\set VERBOSITY verbose\nSET semloom_pg.gateway_socket='$socket'; SELECT ai_semantic.map(repeat('x',163841), 'Echo the input.', $options) FROM ONLY map_rows WHERE id=1");
like($stderr, qr/ERROR:  54000: SemLoom provider input exceeds the 163840 byte limit\n/, 'input preflight rejects the first excess byte');
ok(!IO::Select->new($sentinel)->can_read(0), 'oversized input is rejected before encoding/open');
close($sentinel);
unlink($socket) or die 'cannot remove owned no-task sentinel';
($status, $stdout, $stderr) = run_query("SELECT ai_semantic.map(repeat('x',163840), 'Echo the input.', $options) FROM ONLY map_rows WHERE id=1",
    $peer_script);
is($status, 0, 'the full 163840-byte input reaches the validated v5 task') or diag($stderr);
is($stdout, 'hello', 'large input is processed without changing result policy');

($status, $stdout, $stderr) = golden_query("\\set ON_ERROR_STOP 0\nBEGIN; SAVEPOINT before_cancel; SET LOCAL statement_timeout='50ms'; INSERT INTO map_sink $query WHERE id=1; ROLLBACK TO SAVEPOINT before_cancel; SET LOCAL statement_timeout='5s'; $query WHERE id=1; ROLLBACK; SELECT 1;",
    '--test-response-delay-ms', '150', '--test-max-sessions', '2');
like($stderr, qr/ERROR:  57014: canceling statement due to statement timeout\n/, 'Map waiting for a result preserves native cancellation');
my @errors = $stderr =~ /ERROR:/g;
is(scalar(@errors), 1, 'cancel/savepoint recovery has no secondary errors');
is($stdout, "1|hello\n1", 'same backend can execute Map after cancel and savepoint recovery');

$node->stop;
done_testing();
