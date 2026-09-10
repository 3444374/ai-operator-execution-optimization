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

my $node = PostgreSQL::Test::Cluster->new('predicate_prefetch');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs(id integer, movie text, body text);
INSERT INTO inputs VALUES (1,'chosen','slow'),(2,'chosen','fast'),(3,'other',NULL);
CREATE TABLE limited(id integer, body text);
INSERT INTO limited VALUES (1,'one'),(2,repeat('x',70000));
CREATE FUNCTION risky(x integer) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS
$$BEGIN IF x=2 THEN RAISE EXCEPTION 'later predicate error'; END IF; RETURN true; END$$;
CREATE DOMAIN positive_number AS integer CHECK(VALUE>0);
CREATE TABLE domains(id positive_number, body text);
INSERT INTO domains VALUES (1,'one');
CREATE SEQUENCE evaluations;
CREATE FUNCTION touch_input(x text) RETURNS text LANGUAGE plpgsql VOLATILE STRICT AS
$$BEGIN PERFORM nextval('evaluations'); RETURN x; END$$;
CREATE ROLE prefetch_reader;
CREATE TABLE protected(id integer, allowed boolean, body text);
INSERT INTO protected VALUES (1,true,'visible'),(2,false,'forbidden-payload');
ALTER TABLE protected ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible_rows ON protected USING (allowed);
GRANT SELECT ON protected TO prefetch_reader;
GRANT USAGE ON SCHEMA ai_semantic TO prefetch_reader;
GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO prefetch_reader;
CREATE VIEW guarded WITH (security_barrier=true) AS SELECT id,body FROM protected WHERE allowed;
});
my $socket = $node->host . '/prefetch.sock';
my $events = $node->basedir . '/events.jsonl';
sub outbound_events {
    return join "\n",grep {
        my $event=decode_json($_)->{event};
        $event eq 'job_opened' || $event eq 'offer' || $event eq 'map_task' ||
        $event eq 'submitted' || $event eq 'model_start'
    } split /\n/,(-e $events ? slurp_file($events) : '');
}
my $script = abs_path("$FindBin::RealBin/fixtures/incremental_gateway.py");
my ($out,$err) = ('','');
my $gateway;
END { my $status=$?; eval {$gateway->kill_kill} if defined $gateway; $?=$status; }
$gateway = IPC::Run::start(['python3',$script,'--socket',$socket,'--events',$events],
    '>',\$out,'2>',\$err,IPC::Run::timeout(60));
for (1..500) { last if -S $socket; sleep(.01); }
ok(-S $socket,'fixture ready') or die $err;
my $settings = "SET statement_timeout='8s'; SET semloom_pg.gateway_socket='$socket'; SET semloom_pg.provider_execution_profile='incremental-map';";
my $enabled = "$settings SET semloom_pg.enable_predicate_prefetch=on;";
my $map = q|ai_semantic.map(body,'Echo.','{"model":"model","temperature":0,"max_tokens":128}'::jsonb)|;
my $query = "SELECT id,$map FROM inputs WHERE id<=2";
my $plan = $node->safe_psql('postgres',"$settings EXPLAIN (COSTS OFF) $query");
like($plan,qr/Semantic Input Window: 1/,'new predicate prefetch stays disabled by default');
like($plan,qr/predicate prefetch is disabled/,'EXPLAIN explains default fallback');
$plan = $node->safe_psql('postgres',"$enabled EXPLAIN (COSTS OFF) $query");
like($plan,qr/Semantic Input Window: 2/,'explicit safe integer comparison enables a real window');
unlike($plan,qr/Fallback Reason/,'supported plan does not invent a fallback');
is($node->safe_psql('postgres',"$enabled $query"),"1|mapped:slow\n2|mapped:fast",'safe WHERE supports reverse completion and input-order restoration');
is($node->safe_psql('postgres',"$enabled SELECT id,$map FROM inputs WHERE movie='chosen' AND body IS NOT NULL AND (id<2 OR id>=2)"),
    "1|mapped:slow\n2|mapped:fast",'text equality, NULL and boolean combinations keep the ordinary filter');
is($node->safe_psql('postgres',"$enabled SELECT id,$map FROM inputs WHERE id<0"),'','zero selectivity returns no rows');
my $before = outbound_events();
is($node->safe_psql('postgres',"$enabled SELECT id,$map FROM inputs LIMIT 0"),'','LIMIT zero returns nothing');
is(outbound_events(),$before,'LIMIT zero sends no task or provider request');
like($node->safe_psql('postgres',"$enabled EXPLAIN (COSTS OFF) SELECT id,$map FROM limited LIMIT 1"),
    qr/strict demand: LIMIT\/OFFSET/,'LIMIT declares strict demand fallback');
is($node->safe_psql('postgres',"$enabled SELECT id,$map FROM limited LIMIT 1"),'1|mapped:one','later oversized input cannot fail LIMIT one');
$node->safe_psql('postgres',"UPDATE limited SET body='model-error' WHERE id=2");
is($node->safe_psql('postgres',"$enabled SELECT id,$map FROM limited LIMIT 1"),'1|mapped:one','later model error cannot fail LIMIT one');
unlike(slurp_file($events),qr/"input": "model-error"/,'unrequested model-error row never leaves PostgreSQL');
my ($status,undef,$error) = $node->psql('postgres',"$enabled SELECT id,$map FROM limited WHERE id=2 LIMIT 1");
isnt($status,0,'the same model-error row fails when actually demanded');
like(slurp_file($events),qr/"input": "model-error"/,'positive error control reached the controlled backend');
like($node->safe_psql('postgres',"$enabled EXPLAIN (COSTS OFF) SELECT id,$map FROM limited WHERE risky(id)"),
    qr/predicate operator, type or expression is outside the whitelist/,'IMMUTABLE does not imply safe prefetch');
is($node->safe_psql('postgres',"$enabled SELECT id,$map FROM limited WHERE risky(id) LIMIT 1"),'1|mapped:one','strict demand does not evaluate later risky predicate');
like($node->safe_psql('postgres',"$enabled EXPLAIN (COSTS OFF) SELECT $map FROM domains WHERE id>0"),
    qr/Semantic Input Window: 1/,'domain types are not inferred to be in the built-in whitelist');
my $volatile = $map; $volatile =~ s/map\(body/map(touch_input(body)/;
is($node->safe_psql('postgres',"$enabled SELECT $volatile FROM limited LIMIT 1"),'mapped:one','volatile input executes on demand');
is($node->safe_psql('postgres','SELECT last_value FROM evaluations'),'1','volatile input is not recomputed');
is($node->safe_psql('postgres',"$enabled SET ROLE prefetch_reader; SELECT id,$map FROM protected"),'1|mapped:visible','RLS filters before the semantic provider');
unlike(slurp_file($events),qr/forbidden-payload/,'inaccessible RLS row was never outbound');
$before=outbound_events();
($status,undef,$error) = $node->psql('postgres',"$enabled SELECT id,$map FROM guarded");
isnt($status,0,'security-barrier view remains an explicitly unsupported carrier shape');
is(outbound_events(),$before,'unsupported security-barrier view cannot leak rows to the provider');
$gateway->signal('TERM');
ok(eval {$gateway->finish},'gateway closes') or diag($err);
my @events=map {decode_json($_)} split /\n/,slurp_file($events);
ok(scalar(grep {$_->{event} eq 'submitted' && $_->{usage}->{active_requests}==2} @events),'safe predicate path reaches two actual in-flight tasks');
my @drained=grep {$_->{event} eq 'job_drained'} @events;
ok(!scalar(grep {my $sum=0; $sum += $_ for values %{$_->{usage}}; $sum != 0} @drained),'normal and failed queries release their resource responsibilities');
$node->stop;
done_testing();
