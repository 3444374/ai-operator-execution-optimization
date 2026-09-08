use strict;
use warnings FATAL => 'all';
use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(encode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $node = PostgreSQL::Test::Cluster->new('incremental_map');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs(id integer, body text);
INSERT INTO inputs VALUES (1,'one'),(2,NULL),(3,'three');
CREATE TABLE outputs(id integer, result text);
CREATE SEQUENCE evaluated;
CREATE FUNCTION touch_input(x text) RETURNS text LANGUAGE plpgsql VOLATILE STRICT
AS $$ BEGIN RETURN x || nextval('evaluated')::text; END $$;
});
my $root = $node->basedir;
my $socket = $node->host . '/incremental.sock';
my $port_file = "$root/model.port";
my $config_file = "$root/model.json";
my $requests = "$root/requests.jsonl";
my $http_script = abs_path("$FindBin::RealBin/fixtures/openai_compatible_server.py");
my $gateway_script = abs_path("$FindBin::RealBin/../../../scripts/services/run_execution_provider_gateway.py");
my ($http_out, $http_err, $gateway_out, $gateway_err) = ('','','','');
my ($http, $gateway);
END { my $status = $?; eval { $gateway->kill_kill } if defined $gateway; eval { $http->kill_kill } if defined $http; $? = $status; }
$http = IPC::Run::start(['python3', $http_script, '--port-file', $port_file,
    '--map-mode', '--model-id', 'model', '--raw-outputs', 'mapped', '--max-requests', '5',
    '--request-log', $requests], '>', \$http_out, '2>', \$http_err, IPC::Run::timeout(60));
for (1..500) { last if -f $port_file; sleep(.01); }
ok(-f $port_file, 'HTTP fixture ready') or die $http_err;
open(my $port_handle, '<', $port_file) or die $!;
my $port = <$port_handle>; close($port_handle);
open(my $config, '>', $config_file) or die $!;
print $config encode_json({endpoint_url=>"http://127.0.0.1:$port/v1/chat/completions", model_id=>'model',timeout_ms=>5000});
close($config);
$gateway = IPC::Run::start(['python3', $gateway_script, '--socket', $socket,
    '--fixed-model-config', $config_file, '--incremental-map', '--max-held-tasks', '1', '--max-active-requests', '1',
    '--test-max-sessions', '3'], '>', \$gateway_out, '2>', \$gateway_err, IPC::Run::timeout(60));
for (1..500) { last if -S $socket; sleep(.01); }
ok(-S $socket, 'incremental gateway ready') or die $gateway_err;
my $settings = "SET statement_timeout='10s'; SET semloom_pg.gateway_socket='$socket'; SET semloom_pg.provider_execution_profile='incremental-map'; SET semloom_pg.provider_window_tasks=1;";
my $options = q|' {"model":"model","temperature":0,"max_tokens":128}'::jsonb|;
my $map = "ai_semantic.map(touch_input(body), 'Echo.', $options)";
is($node->safe_psql('postgres', "$settings SELECT $map FROM inputs LIMIT 0"), '', 'LIMIT zero has no result');
is($node->safe_psql('postgres', "$settings SELECT id,$map FROM inputs WHERE id=2"), '2|', 'NULL stays NULL');
ok(!-e $requests, 'zero controls issue no HTTP request');
is($node->safe_psql('postgres', 'SELECT is_called FROM evaluated'), 'f', 'zero controls do not execute strict non-inlined input');

is($node->safe_psql('postgres', "$settings SELECT id,$map FROM inputs LIMIT 1"), '1|mapped', 'one-row window returns result');
is($node->safe_psql('postgres', 'SELECT last_value FROM evaluated'), '1', 'LIMIT evaluates only emitted Map input');
is($node->safe_psql('postgres', "$settings SELECT id,$map FROM inputs"), "1|mapped\n2|\n3|mapped", 'row order and NULL preserved across tasks');
$node->safe_psql('postgres', "$settings INSERT INTO outputs SELECT id,$map FROM inputs");
is($node->safe_psql('postgres', 'SELECT * FROM outputs ORDER BY id'), "1|mapped\n2|\n3|mapped", 'independent read sees inserted results');
my $gateway_done = eval { $gateway->finish };
if ($@) { $gateway->kill_kill; }
ok($gateway_done, 'gateway drains and exits') or diag($gateway_err);
my $http_done = eval { $http->finish };
if ($@) { $http->kill_kill; }
ok($http_done, 'all five physical requests observed') or diag($http_err);
ok(!-e $socket, 'gateway socket removed');
open(my $log, '<', $requests) or die $!;
my @lines = <$log>; close($log);
is(scalar @lines, 5, 'NULL and LIMIT do not create extra physical work');
my $filter_options = q|' {"model":"model","temperature":0,"max_tokens":8}'::jsonb|;
my ($ret,$out,$err) = $node->psql('postgres', "$settings SELECT id FROM inputs WHERE ai_semantic.filter(body,'True.', $filter_options)");
isnt($ret, 0, 'new profile does not silently execute Filter through old path');
like($err, qr/supports generated Map only/, 'unsupported operator is explicit');
my ($old_ret,$old_out,$old_err) = $node->psql('postgres',
    "SET semloom_pg.provider_execution_profile='incremental-map-window-one'");
isnt($old_ret, 0, 'retired v5 bridge profile is rejected');
like($old_err, qr/invalid value for parameter/, 'retired profile does not silently map to v6');
$node->stop;
done_testing();
