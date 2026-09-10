use strict;
use warnings FATAL => 'all';
use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('map_producer_trace');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE trace_inputs(row_id text, body text);
INSERT INTO trace_inputs VALUES ('first"row','slow'),('second','fast'),('third','slow');
});
my $socket = $node->host . '/trace.sock';
my $events = $node->basedir . '/events.jsonl';
my $script = abs_path("$FindBin::RealBin/fixtures/incremental_gateway.py");
my ($out, $err) = ('','');
my $gateway;
END { my $status=$?; eval {$gateway->kill_kill} if defined $gateway; $?=$status; }
$gateway = IPC::Run::start(['python3',$script,'--socket',$socket,'--events',$events],
    '>',\$out,'2>',\$err,IPC::Run::timeout(60));
for (1..500) { last if -S $socket; select(undef,undef,undef,.01); }
ok(-S $socket,'fixture is ready');
my $settings = "SET statement_timeout='8s'; SET semloom_pg.gateway_socket='$socket'; SET semloom_pg.provider_execution_profile='incremental-map';";
my $query = q|SELECT row_id,ai_semantic.map(body,'Echo.','{"model":"model","temperature":0,"max_tokens":128}'::jsonb) FROM trace_inputs|;
my $expected = "first\"row|mapped:slow\nsecond|mapped:fast\nthird|mapped:slow";
is($node->safe_psql('postgres',"$settings $query"),$expected,'default trace disabled preserves results');
my $before = slurp_file($node->logfile);
unlike($before,qr/SEMLOOM_MAP_BINDING/,'default emits no binding trace');
my $traced = "$settings SET semloom_pg.test_map_binding_id_column='row_id';";
is($node->safe_psql('postgres',"$traced $query; $query"),"$expected\n$expected",'two queries with repeated payloads preserve distinct IDs');
my @records;
for my $line (split /\n/, slurp_file($node->logfile)) {
    push @records,decode_json($1) if $line =~ /LOG:\s+SEMLOOM_MAP_BINDING (\{.*\})$/;
}
my (%offers,%streams);
for my $record (@records) {
    my $key = join(':',@{$record}{qw(backend_pid stream offer)});
    if ($record->{phase} eq 'before_offer') { $offers{$key}=$record; }
    else {
        ok(exists $offers{$key},'accepted task has a prior producer observation');
        is($record->{sequence},$offers{$key}->{sequence},'acceptance sequence matches producer');
        push @{$streams{$record->{stream}}},$offers{$key};
    }
}
is(scalar keys %streams,2,'same backend assigns separate query stream identities');
for my $stream (values %streams) {
    is_deeply([map {$_->{sequence}} @$stream],[0,1,2],'each producer begins at sequence zero');
    is_deeply([map {$_->{row_id}} @$stream],['first"row','second','third'],'original row IDs are recorded before offer');
    is($stream->[0]->{payload_digest},$stream->[2]->{payload_digest},'identical payloads retain separate IDs');
}
my ($ret,undef,$error) = $node->psql('postgres',"$settings SET semloom_pg.test_map_binding_id_column='missing'; $query");
isnt($ret,0,'missing trace ID column rejects query');
like($error,qr/trace ID column is absent/,'trace cannot fabricate row identity');
$gateway->signal('TERM');
ok(eval {$gateway->finish},'gateway closes');
$node->stop;
done_testing();
