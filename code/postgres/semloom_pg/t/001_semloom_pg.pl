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
	my $serve_once = !grep { $_ eq '--test-max-sessions' } @arguments;
	my @command = ('python3', $gateway_script, '--socket', $socket_path);
	push @command, '--once' if $serve_once;
	push @command, @arguments;
	my $gateway = IPC::Run::start(
		\@command,
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

sub error_signature
{
	my ($stderr) = @_;
	my ($sqlstate, $message) =
	  $stderr =~ /^.*?ERROR:\s+([0-9A-Z]{5}):\s+(.+)$/m;
	return ($sqlstate // '', $message // '');
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
	my ($sqlstate, $message) = error_signature($stderr);
	ok($sqlstate ne '' && $message ne '', "$fixture exposes an error signature")
	  or diag($stderr);
	unlike($stderr, qr/alpha/, "$fixture error does not expose the task payload");
	finish_recording_gateway($gateway, $socket_path, $gateway_stderr);
	return ($sqlstate, $message);
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

$node->safe_psql(
	'postgres',
	q{CREATE TABLE semloom_filter_decisions (
		doc_id integer PRIMARY KEY,
		decision text
	);
INSERT INTO semloom_filter_decisions VALUES
	(1, 'true'),
	(2, 'false'),
	(3, 'unknown'),
	(4, NULL),
	(5, 'true');});
is(
	$node->safe_psql(
		'postgres',
		q{PREPARE semloom_filter AS
SELECT doc_id
FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision)
ORDER BY doc_id;
EXECUTE semloom_filter;
DEALLOCATE semloom_filter;}),
	"1\n5",
	'prepared SemFilter preserves exact TRUE/FALSE/UNKNOWN semantics');
is(
	$node->safe_psql(
		'postgres',
		q{SET plan_cache_mode = force_generic_plan;
PREPARE semloom_filter_from(integer) AS
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id >= $1 AND ai_semantic.filter(decision)
ORDER BY doc_id;
EXECUTE semloom_filter_from(2);
ALTER TABLE semloom_filter_decisions ADD COLUMN note text;
EXECUTE semloom_filter_from(2);
DEALLOCATE semloom_filter_from;
RESET plan_cache_mode;}),
	"5\n5",
	'generic SemFilter plan is invalidated and rebuilt after a relation change');
my $filter_limit_explain = $node->safe_psql(
	'postgres',
	q{EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id >= 2 AND ai_semantic.filter(decision)
ORDER BY doc_id
LIMIT 1;});
ok(
	index($filter_limit_explain, 'Limit') <
	  index($filter_limit_explain, 'Custom Scan (SemLoom SemFilter)'),
	'SemFilter executes below LIMIT');
like(
	$filter_limit_explain,
	qr/Accepted Rows: 3.*Emitted Rows: 1/s,
	'SemFilter counters distinguish completed decisions from emitted rows');
is(
	$node->safe_psql(
		'postgres',
		q{SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id >= 2 AND ai_semantic.filter(decision)
ORDER BY doc_id
LIMIT 1;}),
	'5',
	'LIMIT is applied after SemFilter keep/drop semantics');

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
	q{INSERT INTO semloom_documents VALUES ('héllo世界'), (NULL), ('');});

$node->safe_psql(
	'postgres',
	q{CREATE ROLE semloom_reader;
CREATE TABLE semloom_secure (owner_name text, decision text);
ALTER TABLE semloom_secure ENABLE ROW LEVEL SECURITY;
CREATE POLICY semloom_secure_owner ON semloom_secure
	USING (owner_name = current_user::text);
GRANT USAGE ON SCHEMA ai_semantic TO semloom_reader;
GRANT EXECUTE ON FUNCTION ai_semantic.filter(text) TO semloom_reader;
GRANT SELECT ON semloom_secure TO semloom_reader;
INSERT INTO semloom_secure VALUES
	('semloom_reader', 'true'),
	('semloom_reader', 'false'),
	('someone_else', 'true');});
is(
	$node->safe_psql(
		'postgres',
		q{SET ROLE semloom_reader;
SELECT owner_name
FROM semloom_secure
WHERE ai_semantic.filter(decision);
RESET ROLE;}),
	'semloom_reader',
	'ordinary child plan applies RLS before SemFilter emits rows');
is(
	$node->safe_psql('postgres', q{SELECT count(*) FROM semloom_secure;}),
	'3',
	'ordinary SQL remains unaffected by semantic planner hooks');

my $gateway_directory = PostgreSQL::Test::Utils::tempdir_short();
my $gateway_socket = $gateway_directory . '/recording.sock';
my $missing_gateway_socket = $gateway_directory . '/missing.sock';
my $golden_fixture = $gateway_directory . '/golden-fixture.json';
open(my $golden_fixture_handle, '>', $golden_fixture)
  or die "could not create golden fixture";
print $golden_fixture_handle <<'GOLDEN_FIXTURE';
{
  "9f8acc0437722c4c5c13c1c60604eada801eaa915a08baac4bd78c08a578907e": "TRUE",
  "77aa32c3ebbef2d54d590f9b9fc12325f3a9abab37d325d9907fafc1db41d95a": "FALSE",
  "e6ec3d09b71e5fd87683a7f26670067c6f1741d29fa60de6e6d9c2b9a7007b95": "UNKNOWN",
  "8c9b589e71bae83f778e0a9f1b3e185f408f251571ff27459c681045bf084392": "true",
  "2e8b2f2ca69fd7aab5f799d222c7b79703fd8d0398805c8f64e40ed1dfb71f8b": "TRUE",
  "d88c3247d12134898397645ab65a83d839be2bb63fb46af62f97b37fb8b512c1": "TRUE"
}
GOLDEN_FIXTURE
close($golden_fixture_handle);

my @exact_filter_argument_cases = (
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, NULL,
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);},
		'instruction NULL'
	],
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, '',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);},
		'instruction empty'
	],
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, decision,
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);},
		'instruction non-constant'
	],
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, 'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0}'::jsonb);},
		'options missing field'
	],
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, 'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8,"future":true}'::jsonb);},
		'options extra field'
	],
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, 'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":true,"max_tokens":8}'::jsonb);},
		'temperature boolean'
	],
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, 'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0.1,"max_tokens":8}'::jsonb);},
		'temperature nonzero'
	],
	[
		q{SELECT * FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision, 'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8.0}'::jsonb);},
		'max_tokens fractional form'
	],
);

for my $argument_case (@exact_filter_argument_cases)
{
	my ($statement, $label) = @$argument_case;
	($ret, $stdout, $stderr) = $node->psql(
		'postgres',
		"\\set VERBOSITY verbose\n$statement");
	isnt($ret, 0, "$label fails during exact SemFilter planning");
	my ($argument_sqlstate, $argument_message) = error_signature($stderr);
	is($argument_sqlstate, '22023', "$label preserves SQLSTATE 22023")
	  or diag($argument_message);
}

$node->safe_psql(
	'postgres',
	q{CREATE TABLE semloom_exact_filter_inputs (
		doc_id integer PRIMARY KEY,
		payload text
	);
INSERT INTO semloom_exact_filter_inputs VALUES
	(1, 'PostgreSQL is a database.'),
	(2, 'A cat sleeps.'),
	(3, 'Insufficient context'),
		(4, NULL),
		(5, 'PostgreSQL is a database.'),
		(6, 'invalid raw'),
		(7, '数据库系统 PostgreSQL。'),
		(8, ''),
		(9, 'missing golden fixture');});

my $exact_filter_query = q{
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id <= 5 AND ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb)
ORDER BY doc_id;};
my ($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
is(
	$node->safe_psql(
		'postgres',
		"SET semloom_pg.gateway_socket = '$gateway_socket';\n$exact_filter_query"),
	"1\n5",
	'exact SemFilter emits only TRUE and preserves duplicate tuple identity');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 9 AND ai_semantic.filter(
  payload,
  'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);});
isnt($ret, 0, 'valid v3 task error frame fails the exact query');
my ($valid_v3_error_state, $valid_v3_error_message) = error_signature($stderr);
is($valid_v3_error_state, '08P01', 'valid v3 task error preserves SQLSTATE 08P01');
is(
	$valid_v3_error_message,
	'SemLoom provider rejected the protocol message',
	'valid v3 task error uses the existing redacted message');
unlike($stderr, qr/missing golden fixture/, 'valid v3 task error exposes no input');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--test-max-sessions',
	'2',
	'--golden-fixture',
	$golden_fixture);
my $exact_savepoint_session = $node->background_psql(
	'postgres',
	on_error_stop => 0);
my ($exact_savepoint_output, $exact_savepoint_had_error) =
  $exact_savepoint_session->query(
	qq{SET semloom_pg.gateway_socket = '$gateway_socket';
BEGIN;
SAVEPOINT semloom_exact_filter_failure;
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 6 AND ai_semantic.filter(
  payload,
  'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);
ROLLBACK TO SAVEPOINT semloom_exact_filter_failure;
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 1 AND ai_semantic.filter(
  payload,
  'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);
COMMIT;});
ok($exact_savepoint_had_error, 'invalid exact result aborts only its statement');
is(
	$exact_savepoint_output,
	'1',
	'exact SemFilter succeeds after savepoint rollback in the same backend');
$exact_savepoint_session->quit;
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 7 AND ai_semantic.filter(
  payload,
  '输入描述数据库系统。',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);}),
	'7',
	'exact SemFilter preserves Unicode instruction/input across C and Python');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 8 AND ai_semantic.filter(
  payload,
  'Input describes a database system.',
  '{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);}),
	'8',
	'exact SemFilter treats empty text as a non-NULL provider task');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
my $exact_filter_explain = $node->safe_psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '$gateway_socket';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
$exact_filter_query});
like(
	$exact_filter_explain,
	qr/Physical Role: reference.*Physical Algorithm: MODEL_REFERENCE_SYNC_V1.*Model: golden-model-v1.*Accepted Rows: 4.*Emitted Rows: 2/s,
	'exact SemFilter exposes reference identity and keep/drop counters');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

like(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
EXPLAIN (COSTS OFF)
$exact_filter_query}),
	qr/Custom Scan \(SemLoom SemFilter\)/,
	'plain exact SemFilter EXPLAIN does not open the provider');
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb)
LIMIT 0;}),
	'',
	'exact SemFilter LIMIT 0 does not open the provider');
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE payload IS NULL AND ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);}),
	'',
	'exact SemFilter NULL input creates no provider task');

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
is(
	$node->safe_psql(
		'postgres',
		qq{SET plan_cache_mode = force_generic_plan;
SET semloom_pg.gateway_socket = '$gateway_socket';
PREPARE semloom_exact_filter(integer) AS
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id BETWEEN \$1 AND 5 AND ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb)
ORDER BY doc_id;
EXECUTE semloom_exact_filter(5);
DEALLOCATE semloom_exact_filter;
RESET plan_cache_mode;}),
	'5',
	'exact SemFilter supports a generic plan with constant semantic identity');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 6 AND ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);});
isnt($ret, 0, 'invalid exact SemFilter raw output fails closed');
my ($exact_sqlstate, $exact_message) = error_signature($stderr);
is($exact_sqlstate, '22000', 'invalid exact raw output preserves SQLSTATE 22000');
is(
	$exact_message,
	'SemFilter model completion must be TRUE, FALSE, or UNKNOWN',
	'invalid exact raw output preserves its redacted canonical message');
unlike($stderr, qr/invalid raw|true/, 'exact parser error exposes no input or raw output');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

my @exact_wire_error_cases = (
	[
		['--test-completion-fixture', 'v3-open-error'],
		'SemLoom provider rejected the protocol message',
		'valid open error'
	],
	[
		['--test-completion-fixture', 'v3-open-error-sequence'],
		'SemLoom provider returned an invalid wire v3 error frame',
		'open error sequence mismatch'
	],
	[
		['--test-tamper-evidence-digest'],
		'SemLoom provider completion does not match wire v3 task identity',
		'tampered evidence'
	],
	[
		['--test-completion-fixture', 'v3-model-mismatch'],
		'SemLoom provider completion does not match wire v3 task identity',
		'model mismatch'
	],
	[
		['--test-completion-fixture', 'v3-invalid-usage'],
		'SemLoom provider response has an invalid text field',
		'invalid usage'
	],
	[
		['--test-completion-fixture', 'v3-finish-reason'],
		'SemLoom provider completion does not match wire v3 task identity',
		'finish reason mismatch'
	],
	[
		['--test-completion-fixture', 'v3-extra-field'],
		'SemLoom provider returned an unexpected message',
		'extra completion field'
	],
	[
		['--test-completion-fixture', 'v3-error-missing-field'],
		'SemLoom provider returned an invalid wire v3 error frame',
		'missing error field'
	],
	[
		['--test-completion-fixture', 'v3-error-extra-field'],
		'SemLoom provider returned an invalid wire v3 error frame',
		'extra error field'
	],
	[
		['--test-completion-fixture', 'v3-error-sequence'],
		'SemLoom provider returned an invalid wire v3 error frame',
		'error sequence mismatch'
	],
	[
		['--test-completion-fixture', 'v3-error-code'],
		'SemLoom provider returned an invalid wire v3 error frame',
		'unknown error code'
	],
);
for my $wire_error_case (@exact_wire_error_cases)
{
	my ($arguments, $expected_message, $label) = @$wire_error_case;
	($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
	  start_recording_gateway(
		$gateway_socket,
		'--golden-fixture',
		$golden_fixture,
		@$arguments);
	($ret, $stdout, $stderr) = $node->psql(
		'postgres',
		qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 1 AND ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);});
	isnt($ret, 0, "exact SemFilter $label fails closed");
	my ($wire_sqlstate, $wire_message) = error_signature($stderr);
	is($wire_sqlstate, '08P01', "exact SemFilter $label preserves SQLSTATE 08P01");
	is($wire_message, $expected_message, "exact SemFilter $label is redacted");
	unlike($stderr, qr/PostgreSQL is a database\./, "$label exposes no task input");
	finish_recording_gateway(
		$exact_gateway,
		$gateway_socket,
		$exact_gateway_stderr);
}

($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 1 AND ai_semantic.filter(
	repeat(payload, 7000),
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);});
isnt($ret, 0, 'oversized exact SemFilter input fails before provider connection');
my ($exact_limit_sqlstate, $exact_limit_message) = error_signature($stderr);
is($exact_limit_sqlstate, '54000', 'exact input limit preserves SQLSTATE 54000');
is(
	$exact_limit_message,
	'SemLoom provider input exceeds the 163840 byte limit',
	'exact input limit reports the adapter-provided v3 limit');
unlike($stderr, qr/could not connect/, 'exact input limit does not connect');

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture,
	'--test-response-delay-ms',
	'500');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{SET statement_timeout = '100ms';
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 1 AND ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);});
isnt($ret, 0, 'statement timeout interrupts exact SemFilter provider wait');
like(
	$stderr,
	qr/canceling statement due to statement timeout/,
	'exact SemFilter wait preserves PostgreSQL cancellation');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);

($exact_gateway, $exact_gateway_stdout, $exact_gateway_stderr) =
  start_recording_gateway(
	$gateway_socket,
	'--golden-fixture',
	$golden_fixture);
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_exact_filter_inputs
WHERE doc_id = 1 AND ai_semantic.filter(
	payload,
	'Input describes a database system.',
	'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb);}),
	'1',
	'exact SemFilter succeeds after cancellation cleanup');
finish_recording_gateway(
	$exact_gateway,
	$gateway_socket,
	$exact_gateway_stderr);
my $parity_query = q{
SELECT ai_semantic.map(payload)
FROM semloom_documents;};
my $in_process_rows = $node->safe_psql(
	'postgres',
	"SET semloom_pg.gateway_socket = '';\n$parity_query");
my $in_process_explain = $node->safe_psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
$parity_query});
$in_process_explain =~
  s/Provider: in-process-recording/Provider: <recording-adapter>/;

my ($parity_gateway, $parity_gateway_stdout, $parity_gateway_stderr) =
  start_recording_gateway($gateway_socket);
is(
	$node->safe_psql(
		'postgres',
		"SET semloom_pg.gateway_socket = '$gateway_socket';\n$parity_query"),
	$in_process_rows,
	'UDS and in-process adapters emit identical rows across the parity dataset');
finish_recording_gateway(
	$parity_gateway,
	$gateway_socket,
	$parity_gateway_stderr);

($parity_gateway, $parity_gateway_stdout, $parity_gateway_stderr) =
  start_recording_gateway($gateway_socket);
my $uds_explain = $node->safe_psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '$gateway_socket';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
$parity_query});
$uds_explain =~ s/Provider: uds-recording/Provider: <recording-adapter>/;
is(
	$uds_explain,
	$in_process_explain,
	'UDS and in-process adapters preserve the same EXPLAIN shape and counters');
finish_recording_gateway(
	$parity_gateway,
	$gateway_socket,
	$parity_gateway_stderr);

my $filter_parity_query = q{
SELECT doc_id
FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision)
ORDER BY doc_id;};
my $in_process_filter_rows = $node->safe_psql(
	'postgres',
	"SET semloom_pg.gateway_socket = '';\n$filter_parity_query");
my $in_process_filter_explain = $node->safe_psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
$filter_parity_query});
$in_process_filter_explain =~
  s/Provider: in-process-recording/Provider: <recording-adapter>/;

($parity_gateway, $parity_gateway_stdout, $parity_gateway_stderr) =
  start_recording_gateway($gateway_socket);
is(
	$node->safe_psql(
		'postgres',
		"SET semloom_pg.gateway_socket = '$gateway_socket';\n$filter_parity_query"),
	$in_process_filter_rows,
	'SemFilter emits identical rows through UDS and in-process adapters');
finish_recording_gateway(
	$parity_gateway,
	$gateway_socket,
	$parity_gateway_stderr);

($parity_gateway, $parity_gateway_stdout, $parity_gateway_stderr) =
  start_recording_gateway($gateway_socket);
my $uds_filter_explain = $node->safe_psql(
	'postgres',
	qq{SET semloom_pg.gateway_socket = '$gateway_socket';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
$filter_parity_query});
$uds_filter_explain =~ s/Provider: uds-recording/Provider: <recording-adapter>/;
is(
	$uds_filter_explain,
	$in_process_filter_explain,
	'SemFilter adapters preserve the same EXPLAIN shape and lifecycle counters');
like(
	$uds_filter_explain,
	qr/Accepted Rows: 4.*Emitted Rows: 2/s,
	'SemFilter EXPLAIN reports non-NULL decisions and emitted TRUE rows');
finish_recording_gateway(
	$parity_gateway,
	$gateway_socket,
	$parity_gateway_stderr);

my $backend_a_socket = $gateway_directory . '/backend-a.sock';
my $backend_b_socket = $gateway_directory . '/backend-b.sock';
my ($backend_a_gateway, $backend_a_stdout, $backend_a_stderr) =
  start_recording_gateway($backend_a_socket);
my ($backend_b_gateway, $backend_b_stdout, $backend_b_stderr) =
  start_recording_gateway($backend_b_socket);
my $backend_a = $node->background_psql('postgres');
my $backend_b = $node->background_psql('postgres');
$backend_a->query_safe("SET semloom_pg.gateway_socket = '$backend_a_socket';");
$backend_b->query_safe("SET semloom_pg.gateway_socket = '$backend_b_socket';");
is(
	$backend_a->query_safe($filter_parity_query),
	"1\n5",
	'first backend owns an independent SemFilter provider session');
is(
	$backend_b->query_safe(
		q{SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';}),
	'recorded:alpha',
	'second backend owns an independent SemMap provider session');
$backend_a->quit;
$backend_b->quit;
finish_recording_gateway(
	$backend_a_gateway,
	$backend_a_socket,
	$backend_a_stderr);
finish_recording_gateway(
	$backend_b_gateway,
	$backend_b_socket,
	$backend_b_stderr);

my @provider_error_cases = (
	['malformed-json', '08P01', 'SemLoom provider returned invalid JSON'],
	['invalid-utf8', '08P01', 'SemLoom provider returned invalid JSON'],
	['escaped-nul', '08P01', 'SemLoom provider returned invalid JSON'],
	['raw-nul', '08P01', 'SemLoom provider returned invalid JSON'],
	['non-object', '08P01', 'SemLoom provider response must be a JSON object'],
	['missing-field', '08P01', 'SemLoom provider returned an unexpected message'],
	['extra-field', '08P01', 'SemLoom provider returned an unexpected message'],
	['wrong-integer-type', '08P01', 'SemLoom provider response has an invalid integer field'],
	['fractional-integer', '08P01', 'SemLoom provider response has an invalid integer field'],
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

$node->safe_psql(
	'postgres',
	q{INSERT INTO semloom_filter_decisions (doc_id, decision)
VALUES (6, 'invalid');});
my $savepoint_session = $node->background_psql(
	'postgres',
	on_error_stop => 0);
my ($savepoint_output, $savepoint_had_error) = $savepoint_session->query(
	q{SET semloom_pg.gateway_socket = '';
BEGIN;
SAVEPOINT semloom_filter_failure;
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 6 AND ai_semantic.filter(decision);
ROLLBACK TO SAVEPOINT semloom_filter_failure;
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 1 AND ai_semantic.filter(decision);
COMMIT;});
ok($savepoint_had_error, 'invalid SemFilter completion aborts its statement');
is(
	$savepoint_output,
	'1',
	'ROLLBACK TO SAVEPOINT restores semantic execution in the same backend');
$savepoint_session->quit;

my ($filter_error_gateway, $filter_error_stdout, $filter_error_stderr) =
  start_recording_gateway($gateway_socket);
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 6 AND ai_semantic.filter(decision);});
isnt($ret, 0, 'invalid UDS SemFilter completion fails closed');
my ($filter_sqlstate, $filter_message) = error_signature($stderr);
is($filter_sqlstate, '22000', 'invalid SemFilter completion preserves SQLSTATE');
is(
	$filter_message,
	'SemFilter provider completion must be true, false, or unknown',
	'invalid SemFilter completion preserves its canonical message');
finish_recording_gateway(
	$filter_error_gateway,
	$gateway_socket,
	$filter_error_stderr);

($filter_error_gateway, $filter_error_stdout, $filter_error_stderr) =
  start_recording_gateway($gateway_socket);
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 1 AND ai_semantic.filter(decision);}),
	'1',
	'a fresh SemFilter session succeeds after invalid completion cleanup');
finish_recording_gateway(
	$filter_error_gateway,
	$gateway_socket,
	$filter_error_stderr);

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
like(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
EXPLAIN (COSTS OFF)
SELECT doc_id
FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision);}),
	qr/Custom Scan \(SemLoom SemFilter\)/,
	'plain SemFilter EXPLAIN does not open the configured provider');
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT doc_id
FROM semloom_filter_decisions
WHERE ai_semantic.filter(decision)
LIMIT 0;}),
	'',
	'SemFilter LIMIT 0 does not open the configured provider');
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT doc_id
FROM semloom_filter_decisions
WHERE decision IS NULL AND ai_semantic.filter(decision);}),
	'',
	'SemFilter NULL input is dropped without opening the provider');

$node->safe_psql(
	'postgres',
	q{CREATE TABLE semloom_private (decision text);
INSERT INTO semloom_private VALUES ('true');});
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SET ROLE semloom_reader;
SELECT * FROM semloom_private WHERE ai_semantic.filter(decision);});
isnt($ret, 0, 'table permissions are checked before SemFilter execution');
my ($permission_sqlstate, $permission_message) = error_signature($stderr);
is($permission_sqlstate, '42501', 'permission denial preserves PostgreSQL SQLSTATE');
unlike($stderr, qr/could not connect/, 'permission denial does not open the provider');

($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT ai_semantic.map(repeat(payload, 200000))
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'oversized input fails before provider connection');
my ($sqlstate, $message) = error_signature($stderr);
is($sqlstate, '54000', 'oversized input preserves its SQLSTATE');
is(
	$message,
	'SemLoom provider input exceeds the 174080 byte limit',
	'oversized input preserves its pre-encoding limit message');
unlike($stderr, qr/could not connect/, 'oversized input does not attempt a provider connection');

($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'missing UDS provider fails the first non-NULL task');
($sqlstate, $message) = error_signature($stderr);
is($sqlstate, 'XX000', 'missing UDS provider preserves socket-access SQLSTATE');
like(
	$message,
	qr/^could not connect to SemLoom provider socket:/,
	'missing UDS provider preserves its operation prefix');
unlike($stderr, qr/\Q$missing_gateway_socket\E/, 'socket failure does not expose its path');

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
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'tampered completion evidence fails closed');
($sqlstate, $message) = error_signature($stderr);
is($sqlstate, '08P01', 'tampered completion evidence preserves its SQLSTATE');
is(
	$message,
	'SemLoom provider completion evidence digest does not match',
	'tampered completion evidence preserves its redacted message');
unlike($stderr, qr/alpha/, 'digest failure does not echo the task payload');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket, '--test-disconnect-on-task');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT ai_semantic.map(payload)
FROM semloom_documents
WHERE payload = 'alpha';});
isnt($ret, 0, 'gateway disconnect fails the query');
($sqlstate, $message) = error_signature($stderr);
is($sqlstate, '08006', 'gateway disconnect preserves its SQLSTATE');
is(
	$message,
	'SemLoom provider disconnected before completing a frame',
	'disconnect preserves its redacted message');
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

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket, '--test-response-delay-ms', '500');
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	qq{SET statement_timeout = '100ms';
SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 1 AND ai_semantic.filter(decision);});
isnt($ret, 0, 'statement timeout interrupts a SemFilter provider wait');
like(
	$stderr,
	qr/canceling statement due to statement timeout/,
	'SemFilter wait preserves PostgreSQL cancellation');
finish_recording_gateway($gateway, $gateway_socket, $gateway_stderr);

($gateway, $gateway_stdout, $gateway_stderr) =
  start_recording_gateway($gateway_socket);
is(
	$node->safe_psql(
		'postgres',
		qq{SET semloom_pg.gateway_socket = '$gateway_socket';
SELECT doc_id
FROM semloom_filter_decisions
WHERE doc_id = 1 AND ai_semantic.filter(decision);}),
	'1',
	'fresh SemFilter execution succeeds after cancellation and cleanup');
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
	qq{\\set VERBOSITY verbose
SET semloom_pg.gateway_socket = '$missing_gateway_socket';
SELECT ai_semantic.map(payload) FROM semloom_documents;});
isnt($ret, 0, 'UDS provider rejects a non-UTF8 database before connection');
($sqlstate, $message) = error_signature($stderr);
is($sqlstate, '0A000', 'non-UTF8 database rejection preserves its SQLSTATE');
is(
	$message,
	'SemLoom UDS recording provider requires UTF8 database encoding',
	'encoding failure preserves the wire requirement message');
unlike($stderr, qr/could not connect/, 'encoding validation does not attempt a provider connection');

$node->stop;
done_testing();
