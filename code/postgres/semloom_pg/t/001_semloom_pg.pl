use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $gateway_script = abs_path("$FindBin::RealBin/../gateway/recording_gateway.py");

sub start_recording_gateway
{
	my ($socket_path, @arguments) = @_;
	my $stdout = '';
	my $stderr = '';
	my $gateway = IPC::Run::start(
		['python3', $gateway_script, '--socket', $socket_path, '--once', @arguments],
		'>', \$stdout,
		'2>', \$stderr);

	for (1 .. 200)
	{
		last if -S $socket_path;
		sleep(0.01);
	}
	ok(-S $socket_path, 'recording gateway creates its Unix socket')
	  or diag($stderr);
	sleep(0.05);
	return ($gateway, \$stdout, \$stderr);
}

sub finish_recording_gateway
{
	my ($gateway, $socket_path, $stderr) = @_;

	$gateway->finish;
	ok(!-e $socket_path, 'recording gateway removes its Unix socket')
	  or diag($$stderr);
}

sub provider_error_signature
{
	my ($node, $socket_path, $fixture) = @_;
	my ($gateway, $gateway_stdout, $gateway_stderr) =
	  start_recording_gateway(
		$socket_path,
		'--test-completion-fixture',
		$fixture);
	my ($ret, $stdout, $stderr) = $node->psql(
		'postgres',
		qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$socket_path';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
	isnt($ret, 0, "$fixture completion fails closed");
	my ($sqlstate, $message) =
	  $stderr =~ /^ERROR:\s+([0-9A-Z]{5}):\s+(.+)$/m;
	ok(defined($sqlstate) && defined($message), "$fixture exposes an error signature")
	  or diag($stderr);
	unlike($stderr, qr/alpha/, "$fixture error does not expose the task payload");
	finish_recording_gateway($gateway, $socket_path, $gateway_stderr);
	return ($sqlstate // '', $message // '');
}

my $node = PostgreSQL::Test::Cluster->new('semloom_pg');
$node->init;
$node->start;

$node->safe_psql(
	'postgres',
	q{
CREATE EXTENSION semloom_pg;
CREATE TABLE semloom_documents (payload text);
INSERT INTO semloom_documents VALUES ('alpha');
});

my ($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	q{SELECT ai_semantic.map(payload) FROM semloom_documents;});
isnt($ret, 0, 'marker fails closed when the planner hook was not preloaded');
like($stderr, qr/marker was not lowered/, 'fail-closed error identifies the residual marker');

$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->restart;

is(
	$node->safe_psql(
		'postgres',
		q{PREPARE semloom_map AS SELECT ai_semantic.map(payload) FROM semloom_documents;
EXECUTE semloom_map;
DEALLOCATE semloom_map;}),
	'recorded:alpha',
	'prepared SemMap executes through the recording CustomScan');

$node->safe_psql('postgres', q{CREATE TABLE semloom_sink (completion text);});
is(
	$node->safe_psql(
		'postgres',
		q{BEGIN;
INSERT INTO semloom_sink
SELECT ai_semantic.map(payload) FROM semloom_documents;
SELECT completion FROM semloom_sink;
ROLLBACK;
SELECT count(*) FROM semloom_sink;}),
	"recorded:alpha\n0",
	'INSERT SELECT emits mapped rows and rollback leaves the sink empty');
is(
	$node->safe_psql(
		'postgres',
		q{INSERT INTO semloom_sink
SELECT ai_semantic.map(payload) FROM semloom_documents;
SELECT completion FROM semloom_sink;}),
	'recorded:alpha',
	'committed INSERT SELECT persists the mapped row');

($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	q{INSERT INTO semloom_sink
SELECT ai_semantic.map(payload) FROM semloom_documents
RETURNING completion;});
isnt($ret, 0, 'INSERT SELECT RETURNING remains fail-closed');
like($stderr, qr/INSERT shape is outside/, 'RETURNING failure identifies the INSERT boundary');

($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	q{INSERT INTO semloom_sink
SELECT ai_semantic.map(payload) FROM semloom_documents
ON CONFLICT DO NOTHING;});
isnt($ret, 0, 'INSERT SELECT ON CONFLICT remains fail-closed');
like($stderr, qr/INSERT shape is outside/, 'ON CONFLICT failure identifies the INSERT boundary');
is(
	$node->safe_psql('postgres', q{SELECT count(*) FROM semloom_sink;}),
	'1',
	'failed INSERT variants leave the committed sink unchanged');

($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	q{SET statement_timeout = '100ms';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE pg_sleep(10) IS NULL;});
isnt($ret, 0, 'statement timeout cancels work in the ordinary child plan');
like($stderr, qr/canceling statement due to statement timeout/, 'cancel is reported to the client');
is(
	$node->safe_psql(
		'postgres',
		q{SELECT ai_semantic.map(payload) FROM semloom_documents;}),
	'recorded:alpha',
	'normal execution succeeds after cancellation');

my $snapshot_session = $node->background_psql('postgres');
$snapshot_session->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ;');
is(
	$snapshot_session->query(
		q{SELECT ai_semantic.map(payload) FROM semloom_documents;}),
	'recorded:alpha',
	'first SemMap read observes the transaction snapshot');
$node->safe_psql('postgres', q{INSERT INTO semloom_documents VALUES ('beta');});
is(
	$snapshot_session->query(
		q{SELECT ai_semantic.map(payload) FROM semloom_documents;}),
	'recorded:alpha',
	'repeated SemMap read preserves the repeatable-read snapshot');
$snapshot_session->query_safe('COMMIT;');
$snapshot_session->quit;

like(
	$node->safe_psql(
		'postgres',
		q{SELECT ai_semantic.map(payload) FROM semloom_documents;}),
	qr/^recorded:alpha\nrecorded:beta$/,
	'a new snapshot observes the committed row');
$node->safe_psql(
	'postgres',
	q{INSERT INTO semloom_documents VALUES ('héllo世界'), (NULL);});

my $gateway_directory = PostgreSQL::Test::Utils::tempdir_short();
my $gateway_socket = $gateway_directory . '/recording.sock';
my $missing_gateway_socket = $gateway_directory . '/missing.sock';

my @provider_error_cases = (
	['malformed-json', '08P01', 'SemLoom provider returned invalid JSON'],
	['invalid-utf8', '08P01', 'SemLoom provider returned invalid JSON'],
	['non-object', '08P01', 'SemLoom provider response must be a JSON object'],
	['missing-field', '08P01', 'SemLoom provider returned an unexpected message'],
	['extra-field', '08P01', 'SemLoom provider returned an unexpected message'],
	['wrong-integer-type', '08P01', 'SemLoom provider response has an invalid integer field'],
	['integer-overflow', '22003', 'integer out of range'],
	['identity-mismatch', '08P01', 'SemLoom provider completion identity does not match the task'],
	['error-message', '08P01', 'SemLoom provider rejected the protocol message'],
);

for my $error_case (@provider_error_cases)
{
	my ($fixture, $expected_sqlstate, $expected_message) = @$error_case;
	my ($sqlstate, $message) =
	  provider_error_signature($node, $gateway_socket, $fixture);
	is($sqlstate, $expected_sqlstate, "$fixture preserves its SQLSTATE");
	is($message, $expected_message, "$fixture preserves its redacted message");
}

like(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
EXPLAIN (COSTS OFF)
SELECT ai_semantic.map(payload) FROM semloom_documents;}),
	qr/Custom Scan \(SemLoom SemMap\)/,
	'plain EXPLAIN does not open the configured provider');
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT ai_semantic.map(payload) FROM semloom_documents LIMIT 0;}),
	'',
	'LIMIT 0 does not open the configured provider');
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE false;}),
	'',
	'a zero-row child plan does not open the configured provider');
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
TRUNCATE semloom_sink;
INSERT INTO semloom_sink
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload IS NULL;
SELECT count(*) FROM semloom_sink WHERE completion IS NULL;}),
	'1',
	'PROPAGATE_NULL is owned by PostgreSQL without opening the provider');

($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT ai_semantic.map(repeat(payload, 200000))
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'oversized input fails before provider connection');
like($stderr, qr/input exceeds .* byte limit/, 'oversized input reports its pre-encoding limit');
unlike($stderr, qr/could not connect/, 'oversized input does not attempt a provider connection');

my ($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket);
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'héllo世界';}),
	'recorded:héllo世界',
	'UDS recording provider preserves Unicode and cross-language digests');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket);
like(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';}),
	qr/Provider: uds-recording/,
	'EXPLAIN identifies the external UDS recording provider');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket, '--test-fill-connect-queue-ms', '500');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{SET statement_timeout = '100ms';
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'statement timeout interrupts provider connect wait');
like($stderr, qr/canceling statement due to statement timeout/, 'connect wait preserves PostgreSQL cancellation');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket, '--test-tamper-evidence-digest');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'tampered completion evidence fails closed');
like($stderr, qr/evidence digest does not match/, 'digest failure is reported without payload text');
unlike($stderr, qr/alpha/, 'digest failure does not echo the task payload');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket, '--test-disconnect-on-task');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'gateway disconnect fails the query');
like($stderr, qr/disconnected before completing a frame/, 'disconnect does not guess task acceptance');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket, '--test-response-delay-ms', '500');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{SET statement_timeout = '100ms';
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'statement timeout interrupts a provider socket wait');
like($stderr, qr/canceling statement due to statement timeout/, 'UDS wait preserves PostgreSQL cancellation');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket);
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';}),
	'recorded:alpha',
	'fresh UDS execution succeeds after cancellation and cleanup');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

$node->safe_psql(
	'postgres',
	q{CREATE DATABASE semloom_latin1
ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0;});
$node->safe_psql(
	'semloom_latin1',
	q{CREATE EXTENSION semloom_pg;
CREATE TABLE semloom_documents (payload text);
INSERT INTO semloom_documents VALUES ('alpha');});
($ret, $stdout, $stderr) = $node->psql(
	'semloom_latin1',
	qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT ai_semantic.map(payload) FROM semloom_documents;});
isnt($ret, 0, 'UDS provider rejects a non-UTF8 database before connection');
like($stderr, qr/requires UTF8 database encoding/, 'encoding failure states the wire requirement');
unlike($stderr, qr/could not connect/, 'encoding validation does not attempt a provider connection');

$node->stop;
done_testing();
