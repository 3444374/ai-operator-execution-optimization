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

my $node=PostgreSQL::Test::Cluster->new('filter_count');
$node->init;
$node->append_conf('postgresql.conf',"shared_preload_libraries='semloom_pg'\n");
$node->start;
$node->safe_psql('postgres',q{
CREATE EXTENSION semloom_pg;
CREATE TABLE reviews(id text PRIMARY KEY, movie text, body text);
INSERT INTO reviews VALUES ('a','chosen','TRUE'),('b','chosen','FALSE'),('c','chosen','TRUE'),
    ('d','chosen',NULL),('e','hidden','forbidden-payload');
CREATE TABLE no_reviews(LIKE reviews);
CREATE ROLE count_reader;
ALTER TABLE reviews ENABLE ROW LEVEL SECURITY;
CREATE POLICY chosen_rows ON reviews USING(movie='chosen');
GRANT SELECT(movie,body) ON reviews TO count_reader;
GRANT USAGE ON SCHEMA ai_semantic TO count_reader;
GRANT EXECUTE ON FUNCTION ai_semantic.filter(text,text,jsonb) TO count_reader;
});
my $root=$node->basedir;
my $socket=$node->host.'/count.sock';
my ($port_file,$config_file,$events,$requests)=map {"$root/$_"} qw(model.port model.json events.jsonl requests.jsonl);
my $fixture=abs_path("$FindBin::RealBin/fixtures/openai_compatible_server.py");
local $ENV{PYTHONPATH}=abs_path("$FindBin::RealBin/../../..");
my ($http,$gateway);my ($ho,$he,$go,$ge)=('','','','');
END {my $s=$?;eval {$http->kill_kill} if defined $http;eval {$gateway->kill_kill} if defined $gateway;$?=$s;}
$http=IPC::Run::start(['python3',$fixture,'--port-file',$port_file,'--model-id','model',
    '--echo-input','--max-requests','11','--request-log',$requests],'>',\$ho,'2>',\$he,IPC::Run::timeout(80));
for (1..500) {last if -f $port_file;sleep(.01);}
ok(-f $port_file,'HTTP fixture ready') or die $he;
my $port=slurp_file($port_file);
open(my $file,'>',$config_file) or die $!;
print $file encode_json({endpoint_url=>"http://127.0.0.1:$port/v1/chat/completions",model_id=>'model',timeout_ms=>3000});close($file);
$gateway=IPC::Run::start(['python3','-m','src.experiments.choice_gateway_observer','--fixture-only',
    '--events',$events,'--','--socket',$socket,'--fixed-model-config',$config_file,
    '--incremental-map','--max-held-tasks','4','--max-active-requests','2'],'>',\$go,'2>',\$ge,IPC::Run::timeout(80));
for (1..500) {last if -S $socket;sleep(.01);}
ok(-S $socket,'production gateway ready') or die $ge;
my $settings="SET statement_timeout='8s'; SET semloom_pg.gateway_socket='$socket'; SET semloom_pg.provider_execution_profile='query-job';";
my $enabled="$settings SET semloom_pg.enable_filter_count=on; SET semloom_pg.test_filter_binding_id_column='id';";
my $predicate=q|ai_semantic.filter(body,'The review is positive.','{"model":"model","temperature":0,"max_tokens":8}'::jsonb)|;
my $count="SELECT count(*) FROM ONLY reviews WHERE movie='chosen' AND $predicate";
my ($status,undef,$error)=$node->psql('postgres',"$settings EXPLAIN $count");
isnt($status,0,'COUNT remains disabled by default');
ok(!-e $requests,'planning did not invoke HTTP');
my $plan=$node->safe_psql('postgres',"$enabled EXPLAIN (COSTS OFF) $count");
like($plan,qr/Aggregate/,'PostgreSQL owns the aggregate');
like($plan,qr/Custom Scan \(SemLoom SemFilter\)/,'semantic filter remains its child');
is($node->safe_psql('postgres',"$enabled $count"),'2','COUNT consumes exactly the kept rows');
my $before=slurp_file($requests);
is($node->safe_psql('postgres',"$enabled SELECT count(*) FROM ONLY reviews WHERE false AND $predicate"),'0','empty selection produces PostgreSQL zero');
is($node->safe_psql('postgres',"$enabled SELECT count(*) FROM ONLY no_reviews WHERE $predicate"),'0','empty relation produces PostgreSQL zero');
is($node->safe_psql('postgres',"$enabled SELECT count(*) FROM ONLY reviews WHERE id='d' AND $predicate"),'0','NULL Filter input creates no task');
is($node->safe_psql('postgres',"$enabled $count LIMIT 0"),'','LIMIT zero skips the whole aggregate');
is(slurp_file($requests),$before,'all no-task COUNT variants send no request');
for my $projection ('sum(length(body))','count(body)','count(DISTINCT body)','count(*) FILTER (WHERE body IS NOT NULL)') {
    ($status,undef,$error)=$node->psql('postgres',"$enabled SELECT $projection FROM ONLY reviews WHERE $predicate");
    isnt($status,0,"unsupported aggregate stays explicit: $projection");
}
($status,undef,$error)=$node->psql('postgres',"$enabled SET ROLE count_reader; $count");
isnt($status,0,'diagnostic row ID requires column SELECT permission');
like($error,qr/permission denied/,'trace privilege error is enforced by PostgreSQL');
is(slurp_file($requests),$before,'unsupported shapes and denied trace have no outbound work');
is($node->safe_psql('postgres',"$settings SET semloom_pg.enable_filter_count=on; SET ROLE count_reader; $count"),'2','COUNT without trace retains ordinary column grants and RLS');
unlike(slurp_file($requests),qr/forbidden-payload/,'RLS-hidden input never reaches HTTP');
$node->safe_psql('postgres',q{UPDATE reviews SET body='bad' WHERE id='c'});
($status,undef,$error)=$node->psql('postgres',"$enabled $count");
isnt($status,0,'late invalid semantic result fails the aggregate');
is($node->safe_psql('postgres',"$enabled SELECT count(*) FROM ONLY reviews WHERE id='a' AND $predicate"),'1','next query works after failed aggregate cleanup');
is($node->safe_psql('postgres',"$enabled SELECT id FROM ONLY reviews WHERE movie='chosen' AND $predicate LIMIT 1"),'a','LIMIT query remains strict demand');
ok(eval {$http->finish},'exactly eleven fixture POSTs completed') or diag($he);
$gateway->signal('TERM');ok(eval {$gateway->finish},'gateway drains') or diag($ge);
my @bindings=map {decode_json($_)} slurp_file($node->logfile)=~/SEMLOOM_FILTER_BINDING (\{[^\n]*\})/g;
my @first=grep {$_->{stream}==1} @bindings;
my @offered=grep {$_->{phase} eq 'before_offer'} @first;
my @decisions=grep {$_->{phase} eq 'decision'} @first;
# Every psql invocation has a new backend; select only the first COUNT backend.
my $pid=$bindings[0]->{backend_pid};
@offered=grep {$_->{phase} eq 'before_offer' && $_->{backend_pid}==$pid} @bindings;
@decisions=grep {$_->{phase} eq 'decision' && $_->{backend_pid}==$pid} @bindings;
is_deeply([map {$_->{row_id}} @offered],[qw(a b c)],'producer binds every evaluated review independently of COUNT output');
is_deeply([map {$_->{kept}?1:0} @decisions],[1,0,1],'row-level decisions explain the aggregate');
my @events=map {decode_json($_)} split /\n/,slurp_file($events);
my @drained=grep {$_->{event} eq 'core_job_drained'} @events;
ok(@drained>=5,'successful and failed queries all reached drain observations');
ok(!scalar(grep {my $n=0;$n+=$_ for values %{$_->{usage}};$n!=0} @drained),'aggregate cleanup releases all responsibilities');
$node->stop;
done_testing();
