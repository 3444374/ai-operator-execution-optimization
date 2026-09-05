use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(decode_json encode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $node = PostgreSQL::Test::Cluster->new('filter_insert');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE filter_source(id integer, decision text);
INSERT INTO filter_source VALUES
    (1,'true'), (2,'false'), (3,'unknown'), (4,NULL), (5,'true');
CREATE TABLE filter_sink(id integer);
});
my $insert = q{INSERT INTO filter_sink
SELECT id + 10 FROM filter_source WHERE id > 1 AND ai_semantic.filter(decision)};
my $plan = $node->safe_psql('postgres', "EXPLAIN (COSTS OFF) $insert");
like($plan, qr/Insert on filter_sink.*Custom Scan \(SemLoom SemFilter\)/s,
    'INSERT plans Filter below the PostgreSQL write node');
my ($status, $stdout, $stderr) = $node->psql('postgres', $insert);
is($status, 0, 'direct Filter INSERT executes') or diag($stderr);
is($node->safe_psql('postgres', 'SELECT id FROM filter_sink ORDER BY id'), '15',
    'ordinary predicate, projection and tristate Filter select the inserted row');

my $socket = $node->host . '/insert.sock';
my $fixture = $node->basedir . '/insert.json';
open(my $file, '>', $fixture) or die 'cannot create INSERT fixture';
print $file encode_json({
    '73327664286f9fd644c0615b0aa2bd4a09399b5723876c5c5035da479a3d3303' => 'TRUE',
    'a3c5646758b13aa3facec172193c98abccf617b263cddafe61363c787a99a38b' => 'FALSE',
    'd3542ab9ed3c1db3001565b26defb30bb02932788640470bfb98c718d099a3ed' => 'UNKNOWN',
    'ab8f0670a52b24c31a1616fce9aa467ae9be57270a13d91c1b1c266f7e9bfc54' => 'bad',
    'cbfb0039089bfcb4bc643e8ee2ad0799121ea841910443bf8b3e52ae4bb6d405' => 'TRUE',
    '9a1fb3a57605eb502b980743847afadc4e9c9b06ea72d01413e349be1a6729c9' => 'FALSE',
    '64bb164fbbd31d41ed09f43fffa5bae1b03b0fd007a8df61c6db3f4cbfa784d8' => 'UNKNOWN',
    'fc3a636c28e5a5a0466e57efcfe9c3dc84a75e81b558a4b0f4f53c0e34a50120' => 'bad',
});
close($file);
my $gateway = abs_path("$FindBin::RealBin/../../../scripts/services/run_execution_provider_gateway.py");
my $legacy_options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb|;
my $choice_options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|;
my @profiles = (
    ['recording', 'ai_semantic.filter(decision)', 0],
    ['exact v3', "ai_semantic.filter(decision, 'Classify input.', $legacy_options)", 1],
    ['choice v4', "ai_semantic.filter(decision, 'Classify input.', $choice_options)", 1],
);

sub execute_insert
{
    my ($external, $sql, $sessions, @arguments) = @_;
    my ($process, $out, $err) = (undef, '', '');
    if ($external)
    {
        $process = IPC::Run::start(['python3', $gateway, '--socket', $socket,
            '--golden-fixture', $fixture, '--test-max-sessions', $sessions, @arguments],
            '>', \$out, '2>', \$err, IPC::Run::timeout(10));
        for (1 .. 200) { last if -S $socket; sleep(0.01); }
        ok(-S $socket, 'INSERT gateway is ready') or diag($err);
    }
    my $path = $external ? $socket : '';
    my @result = $node->psql('postgres', "\\set VERBOSITY verbose\nSET statement_timeout='5s'; SET semloom_pg.gateway_socket='$path';\n$sql");
    if ($process)
    {
        my $exited = eval { $process->finish };
        if ($@) { $process->kill_kill; diag('INSERT gateway exceeded its time limit'); }
        ok($exited, 'INSERT gateway exits normally') or diag($err);
        ok(!-e $socket, 'INSERT gateway removes its listener');
    }
    return @result;
}

sub filter_plans
{
    my ($plan) = @_;
    my @found = ($plan->{'Custom Plan Provider'} // '') eq 'SemLoom SemFilter' ? ($plan) : ();
    for my $child (@{$plan->{'Plans'} // []}) { push @found, filter_plans($child); }
    return @found;
}

$node->safe_psql('postgres', q{
TRUNCATE filter_sink;
INSERT INTO filter_source VALUES (6,'true'), (7,'bad');
CREATE TABLE checked_sink(id integer CHECK(id < 6));
});
for my $profile (@profiles)
{
    my ($label, $predicate, $external) = @$profile;
    my $select = "SELECT id FROM filter_source WHERE id < 7 AND $predicate";
    my $write = "INSERT INTO filter_sink $select";
    my $select_plan = decode_json($node->safe_psql('postgres', "EXPLAIN (FORMAT JSON) $select"))->[0]->{'Plan'};
    my $write_plan = decode_json($node->safe_psql('postgres', "EXPLAIN (FORMAT JSON) $write"))->[0]->{'Plan'};
    my @filters = filter_plans($write_plan);
    is($write_plan->{'Node Type'}, 'ModifyTable', "$label keeps PostgreSQL write ownership");
    is(scalar(@filters), 1, "$label INSERT has exactly one Filter");
    my @selected_filters = filter_plans($select_plan);
    is($filters[0]->{'Physical Role'}, 'reference', "$label keeps the reference role");
    if ($external)
    {
        ok(defined $filters[0]->{'Semantic Spec'}, "$label exposes its semantic spec");
        is($filters[0]->{'Semantic Spec'}, $selected_filters[0]->{'Semantic Spec'},
            "$label INSERT preserves the SELECT spec");
    }
    if ($label eq 'choice v4')
    {
        is($filters[0]->{'Semantic Spec Digest'},
            '3624a95a096a8a6b9e838676ec8865315b1f49c27a0e9594cf67a5440792d6c5',
            'choice INSERT keeps the independent semantic digest vector');
    }
    ($status, $stdout, $stderr) = execute_insert($external, "$write; SELECT id FROM filter_sink ORDER BY id;", 1);
    is($status, 0, "$label commits the selected rows") or diag($stderr);
    is($stdout, "1\n5\n6", "$label retains duplicates and drops FALSE/UNKNOWN/NULL");
    $node->safe_psql('postgres', 'TRUNCATE filter_sink');

    ($status, $stdout, $stderr) = execute_insert($external,
        "BEGIN; EXPLAIN (ANALYZE, FORMAT JSON) $write; ROLLBACK;", 1);
    is($status, 0, "$label INSERT reports actual execution counters") or diag($stderr);
    my @executed = filter_plans(decode_json($stdout)->[0]->{'Plan'});
    is($executed[0]->{'Accepted Rows'}, 5, "$label excludes NULL from accepted tasks");
    is($executed[0]->{'Emitted Rows'}, 3, "$label emits only the three TRUE tuples");
    if ($external) { is($executed[0]->{'Model Calls'}, 5, "$label model calls match non-NULL input"); }

    ($status, $stdout, $stderr) = execute_insert($external, qq{
BEGIN;
INSERT INTO filter_sink SELECT id+10 FROM filter_source WHERE id>1 AND id<7 AND $predicate ORDER BY id DESC LIMIT 1;
SELECT id FROM filter_sink;
ROLLBACK;
SELECT count(*) FROM filter_sink;
}, 1);
    is($status, 0, "$label preserves the non-pulled-up ORDER BY/LIMIT source") or diag($stderr);
    is($stdout, "16\n0", "$label applies LIMIT after filtering and rolls back writes");

    ($status, $stdout, $stderr) = execute_insert($external, qq{
SET plan_cache_mode=force_generic_plan;
PREPARE filtered_insert(integer) AS INSERT INTO filter_sink SELECT id FROM filter_source WHERE id=\$1 AND $predicate;
EXECUTE filtered_insert(1);
ALTER TABLE filter_source ADD COLUMN spare integer;
EXECUTE filtered_insert(5);
DEALLOCATE filtered_insert;
SELECT id FROM filter_sink ORDER BY id;
ALTER TABLE filter_source DROP COLUMN spare;
}, 2);
    is($status, 0, "$label prepared INSERT survives plan invalidation") or diag($stderr);
    is($stdout, "1\n5", "$label prepared INSERT retains bindings and identity");
    $node->safe_psql('postgres', 'TRUNCATE filter_sink');

    ($status, $stdout, $stderr) = execute_insert($external, qq{
\\set ON_ERROR_STOP 0
BEGIN;
SAVEPOINT before_failure;
INSERT INTO filter_sink SELECT id FROM filter_source WHERE id>=6 AND $predicate;
ROLLBACK TO SAVEPOINT before_failure;
INSERT INTO filter_sink SELECT id FROM filter_source WHERE id=1 AND $predicate;
COMMIT;
SELECT id FROM filter_sink;
}, 2);
    my @errors = $stderr =~ /ERROR:/g;
    is(scalar(@errors), 1, "$label savepoint has only the expected parser error") or diag($stderr);
    like($stderr, qr/ERROR:  22000:/, "$label preserves parser SQLSTATE");
    is($stdout, '1', "$label rolls back partial INSERT and recovers in the same backend");
    $node->safe_psql('postgres', 'TRUNCATE filter_sink');

    ($status, $stdout, $stderr) = execute_insert($external,
        "INSERT INTO checked_sink $select;", 1);
    isnt($status, 0, "$label target CHECK failure aborts INSERT");
    like($stderr, qr/ERROR:  23514:/, "$label keeps PostgreSQL target constraint errors");
    is($node->safe_psql('postgres', 'SELECT count(*) FROM checked_sink'), '0',
        "$label target constraint failure leaves no partial rows");

    for my $restriction ('id<0', 'decision IS NULL', 'id<7 LIMIT 0')
    {
        my $where = $restriction =~ /LIMIT/ ? "$predicate AND $restriction" : "$restriction AND $predicate";
        ($status, $stdout, $stderr) = $node->psql('postgres',
            "SET semloom_pg.gateway_socket='$socket'; INSERT INTO filter_sink SELECT id FROM filter_source WHERE $where;");
        is($status, 0, "$label no-task INSERT ($restriction) does not connect") or diag($stderr);
    }
    is($node->safe_psql('postgres', 'SELECT count(*) FROM filter_sink'), '0', "$label no-task writes nothing");

    for my $suffix ('RETURNING id', 'ON CONFLICT DO NOTHING')
    {
        for my $source_limit ('', 'LIMIT 1')
        {
            ($status, $stdout, $stderr) = $node->psql('postgres',
                "\\set VERBOSITY verbose\nSET semloom_pg.gateway_socket='$socket'; $write $source_limit $suffix;");
            isnt($status, 0, "$label $source_limit $suffix remains unsupported");
            like($stderr, qr/ERROR:  0A000: INSERT shape is outside/, "$label rejects INSERT variant before provider I/O");
        }
    }
    for my $unsupported (
        "INSERT INTO filter_sink OVERRIDING SYSTEM VALUE $select",
        "INSERT INTO filter_sink SELECT a.id FROM filter_source a JOIN filter_sink b ON a.id=b.id WHERE $predicate",
        "INSERT INTO filter_sink SELECT count(*) FROM filter_source WHERE $predicate")
    {
        ($status, $stdout, $stderr) = $node->psql('postgres',
            "\\set VERBOSITY verbose\nSET semloom_pg.gateway_socket='$socket'; EXPLAIN $unsupported;");
        isnt($status, 0, "$label rejects unsupported INSERT shape at planning");
        like($stderr, qr/ERROR:  0A000:/, "$label unsupported shape retains feature-not-supported error");
    }
}

$node->safe_psql('postgres', q{
CREATE ROLE filter_writer;
CREATE TABLE secure_source(id integer, decision text, owner_name text);
INSERT INTO secure_source VALUES (1,'true','filter_writer'), (2,'bad','other');
ALTER TABLE secure_source ENABLE ROW LEVEL SECURITY;
CREATE POLICY reader_policy ON secure_source USING(owner_name=current_user::text);
GRANT USAGE ON SCHEMA ai_semantic TO filter_writer;
GRANT EXECUTE ON FUNCTION ai_semantic.filter(text,text,jsonb) TO filter_writer;
GRANT SELECT ON secure_source TO filter_writer;
GRANT INSERT,SELECT ON filter_sink TO filter_writer;
});
my $choice = $profiles[2]->[1];
($status, $stdout, $stderr) = execute_insert(1, qq{
SET ROLE filter_writer;
INSERT INTO filter_sink SELECT id FROM secure_source WHERE $choice;
SELECT id FROM filter_sink;
}, 1);
is($status, 0, 'choice INSERT preserves source RLS and target grants') or diag($stderr);
is($stdout, '1', 'RLS-hidden invalid input never reaches the provider');
$node->safe_psql('postgres', 'TRUNCATE filter_sink; REVOKE INSERT ON filter_sink FROM filter_writer;');
($status, $stdout, $stderr) = $node->psql('postgres', "\\set VERBOSITY verbose\nSET semloom_pg.gateway_socket='$socket'; SET ROLE filter_writer; INSERT INTO filter_sink SELECT id FROM secure_source WHERE $choice;");
like($stderr, qr/ERROR:  42501:/, 'missing INSERT privilege is a PostgreSQL permission error');
is($node->safe_psql('postgres', 'SELECT count(*) FROM filter_sink'), '0', 'permission failure writes nothing');

($status, $stdout, $stderr) = execute_insert(1,
    "SET statement_timeout='100ms'; INSERT INTO filter_sink SELECT id FROM filter_source WHERE id=1 AND $choice;", 1,
    '--test-response-delay-ms', '500');
like($stderr, qr/ERROR:  57014:/, 'choice INSERT cancellation preserves PostgreSQL SQLSTATE');
is($node->safe_psql('postgres', 'SELECT count(*) FROM filter_sink'), '0', 'cancelled INSERT leaves no rows');
($status, $stdout, $stderr) = execute_insert(1,
    "INSERT INTO filter_sink SELECT id FROM filter_source WHERE id=1 AND $choice; SELECT id FROM filter_sink;", 1);
is($status, 0, 'choice INSERT recovers after cancellation') or diag($stderr);
is($stdout, '1', 'recovery commits exactly one row');
$node->stop;
done_testing();
