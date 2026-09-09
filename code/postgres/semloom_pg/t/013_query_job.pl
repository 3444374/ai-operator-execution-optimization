use strict;
use warnings FATAL => 'all';
use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(encode_json decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

plan skip_all => 'query membership uses Linux peer credentials' unless $^O eq 'linux';
my $node = PostgreSQL::Test::Cluster->new('query_job');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs(id integer, body text);
INSERT INTO inputs VALUES (1,'TRUE'),(2,'TRUE'),(3,NULL);
CREATE TABLE rejected(id integer CHECK(id<0), body text);
});
my $root = $node->basedir;
my $socket = $node->host . '/query.sock';
my $port_file = "$root/model.port";
my $config_file = "$root/model.json";
my $events_file = "$root/events.jsonl";
my $requests = "$root/requests.jsonl";
my $http_script = abs_path("$FindBin::RealBin/fixtures/openai_compatible_server.py");
my $code = abs_path("$FindBin::RealBin/../../..");
local $ENV{PYTHONPATH} = $code;
my ($http_out, $http_err, $gateway_out, $gateway_err) = ('','','','');
my ($http, $gateway);
END { my $status = $?; eval { $gateway->kill_kill } if defined $gateway; eval { $http->kill_kill } if defined $http; $? = $status; }
$http = IPC::Run::start(['python3', $http_script, '--port-file', $port_file,
    '--mixed-mode', '--model-id', 'model', '--raw-outputs', 'TRUE', '--max-requests', '20',
    '--request-log', $requests], '>', \$http_out, '2>', \$http_err, IPC::Run::timeout(120));
for (1..500) { last if -f $port_file; sleep(.01); }
ok(-f $port_file, 'HTTP fixture ready') or die $http_err;
open(my $port_handle, '<', $port_file) or die $!;
my $port = <$port_handle>; close($port_handle);
open(my $config, '>', $config_file) or die $!;
print $config encode_json({endpoint_url=>"http://127.0.0.1:$port/v1/chat/completions", model_id=>'model',timeout_ms=>5000});
close($config);
$gateway = IPC::Run::start(['python3', '-m', 'src.experiments.choice_gateway_observer',
    '--fixture-only', '--events', $events_file, '--', '--socket', $socket,
    '--fixed-model-config', $config_file, '--incremental-map', '--max-active-jobs', '3',
    '--max-connections', '12', '--max-held-tasks', '6', '--max-active-requests', '2'],
    '>', \$gateway_out, '2>', \$gateway_err, IPC::Run::timeout(120));
for (1..500) { last if -S $socket; sleep(.01); }
ok(-S $socket, 'query gateway ready') or die $gateway_err;
my $settings = "SET statement_timeout='10s'; SET semloom_pg.gateway_socket='$socket'; SET semloom_pg.provider_execution_profile='query-job';";
my $filter_options = q|' {"model":"model","temperature":0,"max_tokens":8}'::jsonb|;
my $map_options = q|' {"model":"model","temperature":0,"max_tokens":128}'::jsonb|;
my $filter = "ai_semantic.filter(body,'The input is TRUE.', $filter_options)";
my $other_filter = "ai_semantic.filter(body,'The input is nonempty.', $filter_options)";
my $map = "ai_semantic.map(body,'Echo.', $map_options)";
my $both = "SELECT id FROM ONLY inputs WHERE $filter AND $other_filter";
my $composed = "SELECT id,$map FROM ONLY inputs WHERE $filter";

sub events {
    return () unless -f $events_file;
    open(my $fh, '<', $events_file) or die $!;
    my @rows = map { decode_json($_) } <$fh>;
    close($fh);
    return @rows;
}
sub count_event {
    my ($kind) = @_;
    return scalar grep { $_->{event} eq "core_$kind" } events();
}
sub drained {
    for (1..500) {
        return 1 if count_event('job_opened') == count_event('job_drained');
        sleep(.01);
    }
    return 0;
}
sub one_job {
    my ($query, $expected, $label) = @_;
    my $before = count_event('job_opened');
    is($node->safe_psql('postgres', "$settings $query"), $expected, $label);
    ok(drained(), "$label drains");
    is(count_event('job_opened') - $before, 1, "$label uses exactly one Job");
}

is($node->safe_psql('postgres', "$settings $composed LIMIT 0"), '', 'LIMIT zero');
is($node->safe_psql('postgres', "$settings $composed AND id=3"), '', 'NULL filter');
$node->safe_psql('postgres', "$settings EXPLAIN $composed");
is(count_event('job_opened'), 0, 'zero-work plans create no external Job');
ok(!-e $requests, 'zero-work plans call no model');
one_job("$both LIMIT 1", '1', 'double Filter');
one_job("$composed LIMIT 1", '1|TRUE', 'Filter Map');
my $session = $node->background_psql('postgres');
$session->query_safe($settings);
$session->query_safe("PREPARE q AS $both LIMIT 1");
my $before = count_event('job_opened');
for (1..2) { is($session->query_safe('EXECUTE q'), '1', 'prepared execution'); ok(drained(), 'prepared run drains'); }
is(count_event('job_opened') - $before, 2, 'same prepared plan has fresh execution Jobs');
$session->query_safe('DEALLOCATE q');
$session->query_safe("BEGIN; DECLARE a CURSOR FOR $composed; DECLARE b CURSOR FOR $composed");
is($session->query_safe('FETCH 1 FROM a'), '1|TRUE', 'first cursor');
is($session->query_safe('FETCH 1 FROM b'), '1|TRUE', 'second cursor');
is(count_event('job_opened') - count_event('job_drained'), 2, 'same backend has two live query Jobs');
$session->query_safe('CLOSE a');
for (1..500) { last if count_event('job_opened') - count_event('job_drained') == 1; sleep(.01); }
is(count_event('job_opened') - count_event('job_drained'), 1, 'closing one cursor retains the other');
$session->query_safe('CLOSE b; COMMIT');
ok(drained(), 'cursor Jobs drain');
$session->query_safe("BEGIN; DECLARE kept CURSOR FOR $composed");
is($session->query_safe('FETCH 1 FROM kept'), '1|TRUE', 'outer cursor before subtransaction');
$session->query_safe('SAVEPOINT s');
$session->query_safe("\\set ON_ERROR_STOP off");
$session->query("INSERT INTO rejected $composed LIMIT 1");
like($session->{stderr}, qr/violates check constraint/, 'expected insert error observed');
$session->{stderr} = '';
$session->query_safe("\\set ON_ERROR_STOP on");
$session->query_safe('ROLLBACK TO s');
is($session->query_safe('FETCH 1 FROM kept'), '2|TRUE', 'subtransaction error does not close outer query');
$session->query_safe('CLOSE kept; COMMIT');
ok(drained(), 'error and outer query both drain');
$session->query_safe("BEGIN; DECLARE abandoned CURSOR FOR $composed");
is($session->query_safe('FETCH 1 FROM abandoned'), '1|TRUE', 'cursor before backend disconnect');
$session->quit;
ok(drained(), 'backend disconnect closes query control');
my @joined = grep { $_->{event} eq 'core_query_flow_joined' } events();
my %flows;
for (@joined) { $flows{$_->{job_id}}{$_->{flow}} = 1; }
ok(scalar(keys %flows) >= 9, 'observed distinct query executions');
for my $job (sort keys %flows) { is(scalar(keys %{$flows{$job}}), 2, "$job has both operator streams"); }
open(my $log, '<', $requests) or die $!;
my @lines = <$log>; close($log);
is(scalar @lines, 20, 'bounded exact HTTP count');
ok(eval { $http->finish }, 'fixture consumed all planned requests') or diag($http_err);
$gateway->signal('TERM');
ok(eval { $gateway->finish }, 'gateway exits after draining') or diag($gateway_err);
ok(!-e $socket, 'gateway socket removed');
$node->stop;
done_testing();
