use strict;
use warnings FATAL => 'all';
use utf8;

use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(decode_json encode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $node = PostgreSQL::Test::Cluster->new('choice_execution');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE choice_rows(id integer, content text);
INSERT INTO choice_rows VALUES (1, U&'\6570\636E\5E93\+01F642'), (2, NULL);
});
my $socket = $node->host . '/choice.sock';
my $fixture = $node->basedir . '/choice.json';
open(my $file, '>', $fixture) or die "cannot create golden fixture";
print $file encode_json({
    '0d587219759ce92992da90a8af1fc40baefff79ab79861d9930886c667dc7fa1' => 'TRUE',
    '65110b7d4e96af485150b0322525fc171a8fcf9ca181639039b10d9648581961' => 'FALSE',
    '0a1d1077829a66b711d2d93890acc97fd084531027347af9cff45428ab5cc326' => 'FALSE',
    '64cf52d53dca13bdf8f2a7ebc377f36102c9d38792b0a399e48d74363f00ba10' => 'UNKNOWN',
    '579517ce46aeb328f695a99dd9a952ae8c158da80b9c19c656a3601c2ff2c5cb' => 'TRUE',
    '865bde3f68492fca1f0b3b733d6a90b6ee864760ec75b8101217ce2bcaef1302' => 'TRUE',
    '2f0862f3ef168265b087632345a24dde26887a8ce945a147e0e540adae5bfd71' => 'true',
});
close($file);
my $gateway_script = abs_path("$FindBin::RealBin/../gateway/recording_gateway.py");
my ($gateway_out, $gateway_err) = ('', '');
my $gateway = IPC::Run::start(['python3', $gateway_script, '--socket', $socket,
    '--once', '--golden-fixture', $fixture], '>', \$gateway_out, '2>', \$gateway_err);
for (1 .. 200) { last if -S $socket; sleep(0.01); }
ok(-S $socket, 'choice golden gateway listens') or diag($gateway_err);
my $options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|;
my ($status, $stdout, $stderr) = $node->psql('postgres', qq{
SET semloom_pg.gateway_socket = '$socket';
SELECT id FROM choice_rows WHERE ai_semantic.filter(content, 'Classify input.', $options);
});
is($status, 0, 'choice executes through PostgreSQL and golden wire v4') or diag($stderr);
is($stdout, '1', 'choice preserves the TRUE row and drops SQL NULL');
if ($status == 0) { $gateway->finish; }
else { $gateway->kill_kill; }

sub start_peer
{
    my ($script, @arguments) = @_;
    my ($out, $err) = ('', '');
    my $process = IPC::Run::start(['python3', $script, '--socket', $socket, @arguments],
        '>', \$out, '2>', \$err, IPC::Run::timeout(10));
    for (1 .. 200) { last if -S $socket; sleep(0.01); }
    ok(-S $socket, 'test peer is listening') or diag($err);
    return ($process, \$err);
}

sub finish_peer
{
    my ($process, $err) = @_;
    my $exited = eval { $process->finish };
    if ($@) { $process->kill_kill; diag('test peer exceeded its time limit'); }
    ok($exited, 'test peer exits successfully') or diag($$err);
    ok(!-e $socket, 'test peer closed its listener') or diag($$err);
}

sub golden_query
{
    my ($sql, @arguments) = @_;
    my @sessions = grep { $_ eq '--test-max-sessions' } @arguments;
    my ($process, $err) = start_peer($gateway_script,
        (@sessions ? () : ('--once')), '--golden-fixture', $fixture, @arguments);
    my @result = $node->psql('postgres', "\\set VERBOSITY verbose\nSET statement_timeout='5s'; SET semloom_pg.gateway_socket='$socket';\n$sql");
    finish_peer($process, $err);
    return @result;
}

my $query = "SELECT id FROM choice_rows WHERE ai_semantic.filter(content, 'Classify input.', $options)";
$node->safe_psql('postgres', q{
INSERT INTO choice_rows VALUES (3, ''), (4, 'negative'), (5, 'uncertain'), (6, 'database'), (7, 'database');
});
($status, $stdout, $stderr) = golden_query("$query ORDER BY id;");
is($status, 0, 'choice handles all three decisions') or diag($stderr);
is($stdout, "1\n6\n7", 'FALSE/UNKNOWN/NULL drop, duplicate TRUE rows remain ordered');
($status, $stdout, $stderr) = golden_query("EXPLAIN (ANALYZE, FORMAT JSON) $query;");
is($status, 0, 'choice EXPLAIN ANALYZE succeeds') or diag($stderr);
my $executed = decode_json($stdout)->[0]->{'Plan'};
is($executed->{'Model Calls'}, 6, 'SQL NULL consumes no task');
is($executed->{'Accepted Rows'}, 6, 'successful task count excludes NULL');
is($executed->{'Emitted Rows'}, 3, 'emitted rows count only TRUE');
is($executed->{'Generation Quality'}, 'unqualified', 'execution does not qualify model quality');
is($executed->{'AI Cost Calibration'}, 'unavailable', 'new identity stays uncalibrated');
($status, $stdout, $stderr) = golden_query("$query LIMIT 1;");
is($status, 0, 'early stop closes the choice session') or diag($stderr);
is($stdout, '1', 'LIMIT sees kept tuples');
my $unicode_query = "SELECT id FROM choice_rows WHERE id=1 AND ai_semantic.filter(content, U&'\\5224\\65AD\\662F\\5426\\662F\\6570\\636E\\5E93\\3002', $options)";
($status, $stdout, $stderr) = golden_query($unicode_query);
is($status, 0, 'Unicode instruction and input survive C/Python canonicalization') or diag($stderr);
is($stdout, '1', 'Unicode choice returns the expected row');

($status, $stdout, $stderr) = golden_query(qq{
SET plan_cache_mode=force_generic_plan;
PREPARE chosen(integer) AS $query AND id=\$1;
EXECUTE chosen(1);
ALTER TABLE choice_rows ADD COLUMN extra integer;
EXECUTE chosen(1);
}, '--test-max-sessions', '2');
is($status, 0, 'prepared choice survives relation invalidation') or diag($stderr);
is($stdout, "1\n1", 'prepared query retains profile and results');

$node->safe_psql('postgres', q{INSERT INTO choice_rows(id,content) VALUES(8,'invalid-label'); CREATE TABLE choice_sink(id integer);});
($status, $stdout, $stderr) = golden_query(qq{
\\set ON_ERROR_STOP 0
BEGIN;
SAVEPOINT before_bad_label;
$query AND id=8;
ROLLBACK TO SAVEPOINT before_bad_label;
INSERT INTO choice_sink VALUES (9);
$query AND id=1;
ROLLBACK;
SELECT count(*) FROM choice_sink;
}, '--test-max-sessions', '2');
like($stderr, qr/ERROR:  22000: SemFilter model completion must be TRUE, FALSE, or UNKNOWN\n/,
    'invalid raw output is rejected by the unchanged PG parser');
unlike($stderr, qr/invalid-label/, 'parser error exposes no input');
my @savepoint_errors = $stderr =~ /ERROR:/g;
is(scalar(@savepoint_errors), 1, 'savepoint recovery has no secondary statement error');
is($stdout, "1\n0", 'same-backend choice recovery and ordinary write rollback preserve PG ownership');

my $peer_script = abs_path("$FindBin::RealBin/fixtures/choice_wire_peer.py");
my @faults = (
    ['open-profile', 'SemLoom provider open response does not match wire v4'],
    ['open-version', 'SemLoom provider open response does not match wire v4'],
    ['open-extra', 'SemLoom provider returned an unexpected message'],
    ['open-missing', 'SemLoom provider returned an unexpected message'],
    ['open-duplicate', 'SemLoom provider returned invalid JSON'],
    ['completion-profile', 'SemLoom provider completion does not match wire v4 task identity'],
    ['completion-version', 'SemLoom provider completion does not match wire v4 task identity'],
    ['completion-sequence', 'SemLoom provider completion does not match wire v4 task identity'],
    ['completion-evidence', 'SemLoom provider completion does not match wire v4 task identity'],
    ['completion-extra', 'SemLoom provider returned an unexpected message'],
    ['completion-missing', 'SemLoom provider returned an unexpected message'],
    ['completion-duplicate', 'SemLoom provider returned invalid JSON'],
    ['completion-escaped-nul', 'SemLoom provider returned invalid JSON'],
    ['completion-raw-nul', 'SemLoom provider returned invalid JSON'],
    ['completion-fractional', 'SemLoom provider completion does not match wire v4 task identity'],
    ['legacy', 'SemLoom provider returned an invalid wire v4 error frame'],
);
for my $mutation (qw(extra missing version sequence code))
{
    push @faults, ["error-$mutation", 'SemLoom provider returned an invalid wire v4 error frame'];
}
push @faults, ['error-duplicate', 'SemLoom provider returned invalid JSON'];
for my $case (@faults)
{
    my ($fault, $message) = @$case;
    my ($process, $err) = start_peer($peer_script, '--golden-fixture', $fixture, '--fault', $fault);
    ($status, $stdout, $stderr) = $node->psql('postgres',
        "\\set VERBOSITY verbose\nSET semloom_pg.gateway_socket='$socket'; $query AND id=1;");
    isnt($status, 0, "$fault fails closed");
    like($stderr, qr/ERROR:  08P01: \Q$message\E\n/, "$fault has an exact redacted error") or diag($stderr);
    unlike($stderr, qr/not-public|database/, "$fault exposes no input or injected field");
    finish_peer($process, $err);
}

($status, $stdout, $stderr) = golden_query("SET statement_timeout='100ms'; $query AND id=1;",
    '--test-response-delay-ms', '500');
isnt($status, 0, 'PG cancels an in-flight choice task');
like($stderr, qr/ERROR:  57014: canceling statement due to statement timeout\n/, 'query cancel keeps its original SQLSTATE');
($status, $stdout, $stderr) = golden_query("$query AND id=1;");
is($status, 0, 'fresh choice session works after cancel and protocol errors') or diag($stderr);
is($stdout, '1', 'recovery preserves SQL results');

my $http_script = abs_path("$FindBin::RealBin/fixtures/openai_compatible_server.py");
my %requests;
for my $mode (qw(legacy choice rejected))
{
    my $port_file = $node->basedir . "/$mode.port";
    my $request_log = $node->basedir . "/$mode.requests";
    my $config_file = $node->basedir . "/$mode.config";
    my ($out, $err) = ('', '');
    my @choice_argument = $mode eq 'legacy' ? () : ('--require-choice');
    my @status_argument = $mode eq 'rejected' ? ('--response-status', '400') : ();
    my $http = IPC::Run::start(['python3', $http_script, '--port-file', $port_file,
        '--model-id', 'golden-model-v1', '--raw-outputs', 'UNKNOWN',
        '--request-log', $request_log, @choice_argument, @status_argument],
        '>', \$out, '2>', \$err);
    for (1 .. 200) { last if -f $port_file; sleep(0.01); }
    ok(-f $port_file, "$mode HTTP fixture is ready") or diag($err);
    open(my $port_handle, '<', $port_file) or die 'cannot read fixture port';
    my $port = <$port_handle>;
    close($port_handle);
    my %config = (endpoint_url => "http://127.0.0.1:$port/v1/chat/completions",
        model_id => 'golden-model-v1', timeout_ms => 1000);
    $config{choice_format} = 'vllm_structured_outputs' if $mode ne 'legacy';
    open(my $config_handle, '>', $config_file) or die 'cannot write fixture config';
    print $config_handle encode_json(\%config);
    close($config_handle);
    my ($process, $peer_err) = start_peer($gateway_script, '--once', '--fixed-model-config', $config_file);
    my $fixed_query = $query . ' AND id <= 2';
    $fixed_query =~ s/,"generation_profile":"[^"]+"// if $mode eq 'legacy';
    ($status, $stdout, $stderr) = $node->psql('postgres', qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket='$socket';
SET semloom_pg.provider_execution_profile='openai-compatible-fixed';
EXPLAIN (ANALYZE, FORMAT JSON) $fixed_query;
});
    if ($mode eq 'rejected')
    {
        isnt($status, 0, 'service rejection does not become FALSE');
        like($stderr, qr/ERROR:  38000: SemLoom model request was rejected\n/,
            'choice service rejection retains the neutral SQLSTATE mapping');
    }
    else
    {
        is($status, 0, "$mode fixed-model path executes") or diag($stderr);
        my $plan = decode_json($stdout)->[0]->{'Plan'};
        is($plan->{'Provider'}, 'uds-openai-compatible-fixed', 'fixed adapter is observable');
        is($plan->{'Model Calls'}, 1, 'HTTP task count excludes SQL NULL');
        is($plan->{'Prompt Tokens'}, 17, 'provider prompt usage is preserved');
        is($plan->{'Output Tokens'}, 1, 'provider output usage is preserved');
        is($plan->{'Emitted Rows'}, 0, 'raw UNKNOWN is parsed and dropped by PostgreSQL');
    }
    finish_peer($process, $peer_err);
    $http->finish;
    ok(!-e $port_file, "$mode HTTP fixture stopped");
    open(my $requests_handle, '<', $request_log) or die 'cannot read request evidence';
    my @lines = <$requests_handle>;
    close($requests_handle);
    is(scalar(@lines), 1, "$mode issued exactly one HTTP request, without fallback retry");
    $requests{$mode} = decode_json($lines[0]);
}
is_deeply(delete $requests{choice}->{structured_outputs}, {choice => ['TRUE', 'FALSE', 'UNKNOWN']},
    'the actual choice request carries the ordered constraint');
is_deeply($requests{choice}, $requests{legacy}, 'actual old/new HTTP requests differ only by the explicit constraint');
is_deeply($requests{rejected}->{structured_outputs}, {choice => ['TRUE', 'FALSE', 'UNKNOWN']},
    'the rejected request was constrained, not silently downgraded');

($status, $stdout, $stderr) = $node->psql('postgres', qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket='/nonexistent/choice-provider.sock';
SELECT id FROM choice_rows WHERE id=1 AND ai_semantic.filter(repeat('x', 163841), 'Classify input.', $options);
});
isnt($status, 0, 'oversized choice input is refused before provider connection');
like($stderr, qr/ERROR:  54000: SemLoom provider input exceeds the 163840 byte limit\n/,
    'choice preserves the encoding preflight and neutral limit error');
$node->stop;
done_testing();
