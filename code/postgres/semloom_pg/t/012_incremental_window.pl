use strict;
use warnings FATAL => 'all';
use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $node = PostgreSQL::Test::Cluster->new('incremental_window');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs(id integer, body text);
INSERT INTO inputs VALUES (1,'slow'),(2,'fast'),(3,NULL);
CREATE TABLE controls(id integer, body text);
INSERT INTO controls VALUES (1,'one'),(2,'two');
CREATE TABLE outputs(id integer, result text);
CREATE SEQUENCE evaluated;
CREATE FUNCTION touch_input(x text) RETURNS text LANGUAGE plpgsql VOLATILE STRICT
AS $$ BEGIN PERFORM nextval('evaluated'); RETURN x; END $$;
});
my $socket = $node->host . '/window.sock';
my $events = $node->basedir . '/events.jsonl';
my $script = abs_path("$FindBin::RealBin/fixtures/incremental_gateway.py");
my ($out, $err) = ('','');
my $gateway;
END { my $status = $?; eval { $gateway->kill_kill } if defined $gateway; $? = $status; }
$gateway = IPC::Run::start(['python3',$script,'--socket',$socket,'--events',$events], '>',\$out,'2>',\$err,IPC::Run::timeout(60));
for (1..500) { last if -S $socket; sleep(.01); }
ok(-S $socket, 'version-six fixture ready') or die $err;
my $settings = "SET statement_timeout='8s'; SET semloom_pg.gateway_socket='$socket'; SET semloom_pg.provider_execution_profile='incremental-map';";
my $map = q|ai_semantic.map(body, 'Echo.', '{"model":"model","temperature":0,"max_tokens":128}'::jsonb)|;
is($node->safe_psql('postgres', "$settings SELECT id,$map FROM inputs LIMIT 0"),'','LIMIT zero does not open provider');
ok(!-e $events, 'LIMIT zero submits no task');
my $plan = $node->safe_psql('postgres', "$settings EXPLAIN (COSTS OFF) SELECT id,$map FROM inputs");
like($plan,qr/Semantic Input Window: 2/,'plain Map uses two retained rows');
is($node->safe_psql('postgres', "$settings SELECT id,body,$map FROM inputs"),"1|slow|mapped:slow\n2|fast|mapped:fast\n3||",'reverse model completion retains input association and row order');
$node->safe_psql('postgres', "$settings INSERT INTO outputs SELECT id,$map FROM inputs");
is($node->safe_psql('postgres','SELECT * FROM outputs ORDER BY id'),"1|mapped:slow\n2|mapped:fast\n3|",'independent read verifies INSERT');
is($node->safe_psql('postgres', "$settings SELECT id,$map FROM controls LIMIT 1"),'1|mapped:one','LIMIT one sends one input');
my $volatile = $map; $volatile =~ s/map\(body/map(touch_input(body)/;
like($node->safe_psql('postgres', "$settings EXPLAIN (COSTS OFF) SELECT $volatile FROM controls"),qr/Semantic Input Window: 1/,'side-effecting input uses window one');
is($node->safe_psql('postgres', "$settings SELECT $volatile FROM controls LIMIT 1"),'mapped:one','fallback input still executes');
is($node->safe_psql('postgres','SELECT last_value FROM evaluated'),'1','volatile input evaluated once');
$node->safe_psql('postgres', "UPDATE controls SET body='cancel' WHERE id=1");
my ($ret,$cancel_out,$cancel_err) = $node->psql('postgres', "$settings SET statement_timeout='100ms'; SELECT $map FROM controls");
isnt($ret,0,'cancellation stops a window with requests in flight');
like($cancel_err,qr/canceling statement due to statement timeout/,'PG owns cancellation');
sleep(.5);
$node->safe_psql('postgres', "UPDATE controls SET body='one' WHERE id=1");
is($node->safe_psql('postgres', "$settings SELECT $map FROM controls WHERE id=1 LIMIT 1"),'mapped:one','same gateway recovers after late response');
$node->safe_psql('postgres', 'CREATE TABLE paging AS SELECT g AS id,repeat(g::text,500) AS body FROM generate_series(1,40) g');
my $expected = join("\n", map {my $body = "$_" x 500; "$_|$body|mapped:$body"} (1..40));
is($node->safe_psql('postgres', "$settings SELECT id,body,$map FROM paging"),$expected,'retained input survives many child pages and slot reuse');
$gateway->signal('TERM');
my $done = eval { $gateway->finish };
ok($done,'gateway closes transport') or diag($err);
open(my $log,'<',$events) or die $!;
my @events = map {decode_json($_)} <$log>; close($log);
ok(scalar(grep {$_->{event} eq 'submitted' && $_->{usage}->{active_requests} == 2} @events),'core observed two simultaneous active requests');
my @ends = map {$_->{input}} grep {$_->{event} eq 'model_end'} @events;
is_deeply([@ends[0,1]],['fast','slow'],'model completes in reverse order');
my @drained = grep {$_->{event} eq 'drained'} @events;
ok(@drained >= 6,'normal and cancelled sessions drained');
ok(!scalar(grep {my $sum=0; $sum += $_ for values %{$_->{usage}}; $sum != 0} @drained),'drained sessions hold no resource credits');
ok(!-e $socket,'gateway socket removed');
for my $fault ('ack-version','sequence','payload') {
    my $fault_socket = $node->host . "/$fault.sock";
    $gateway = IPC::Run::start(['python3',$script,'--socket',$fault_socket,'--events',"$events.$fault",'--fault',$fault], '>',\$out,'2>',\$err,IPC::Run::timeout(30));
    for (1..500) { last if -S $fault_socket; sleep(.01); }
    ok(-S $fault_socket,"$fault fixture ready") or die $err;
    my ($status,$output,$error) = $node->psql('postgres', "$settings SET semloom_pg.gateway_socket='$fault_socket'; SELECT $map FROM controls LIMIT 1", extra_params => ['--set=VERBOSITY=verbose']);
    isnt($status,0,"$fault is rejected");
    like($error,qr/08P01/,"$fault reports protocol error");
    $gateway->signal('TERM');
    ok(eval {$gateway->finish},"$fault gateway closes") or diag($err);
}
$node->stop;
done_testing();
