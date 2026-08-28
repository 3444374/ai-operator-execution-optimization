use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

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

$node->stop;
done_testing();
