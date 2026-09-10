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

my $node = PostgreSQL::Test::Cluster->new('map_backpressure');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs AS SELECT g::text AS row_id, g::text AS body FROM generate_series(1,32) g;
});
my $socket = $node->host . '/pressure.sock';
my $events = $node->basedir . '/events.jsonl';
my $script = abs_path("$FindBin::RealBin/fixtures/incremental_gateway.py");
my ($out,$err) = ('','');
my $gateway;
END { my $status=$?; eval {$gateway->kill_kill} if defined $gateway; $?=$status; }
$gateway = IPC::Run::start(['python3',$script,'--socket',$socket,'--events',$events,
    '--result-bytes','2097152'], '>',\$out,'2>',\$err,IPC::Run::timeout(60));
for (1..500) { last if -S $socket; sleep(.01); }
ok(-S $socket,'bounded result fixture ready') or die $err;
my $settings = "SET statement_timeout='8s'; SET semloom_pg.gateway_socket='$socket'; SET semloom_pg.provider_execution_profile='incremental-map'; SET semloom_pg.provider_window_tasks=16; SET semloom_pg.test_map_binding_id_column='row_id';";
my $query = q|SELECT row_id,ai_semantic.map(body,'Echo.','{"model":"model","temperature":0,"max_tokens":128}'::jsonb) FROM inputs|;
my $expected = join("\n", map {"$_|mapped:$_"} (1..32));
is($node->safe_psql('postgres',"$settings $query"),$expected,'all rows complete in input order despite retained capacity two');
$gateway->signal('TERM');
ok(eval {$gateway->finish},'gateway closes without quarantined work') or diag($err);
open(my $log,'<',$events) or die $!;
my @events = map {decode_json($_)} <$log>; close($log);
my ($blocked,$violations,$offers,$accepted,$rejected) = (0,0,0,0,0);
my @submitted;
for my $event (@events) {
    $blocked=0 if $event->{event} eq 'released';
    push @submitted,$event->{key}->{sequence} if $event->{event} eq 'submitted';
    next unless $event->{event} eq 'offer';
    $offers++;
    $violations++ if $blocked;
    if ($event->{accepted_prefix_count}) { $accepted++; }
    else { $blocked=1; $rejected++; }
}
is($accepted,32,'one accepted input per output');
ok($rejected>0,'test actually encounters result storage pressure');
is($violations,0,'no repeated offer until related result responsibility releases');
cmp_ok($offers,'<=',64,'offer count grows linearly with released tasks');
is_deeply([sort {$a<=>$b} @submitted],[0..31],'no duplicate model submission or consumed rejected sequence');
my @drained = grep {$_->{event} eq 'job_drained'} @events;
is(scalar @drained,1,'query Job drains');
ok(!scalar(grep {my $sum=0; $sum += $_ for values %{$_->{usage}}; $sum != 0} @drained),'no retained resource after query close');
ok(!-e $socket,'socket removed');
$node->stop;
done_testing();
