use strict;
use warnings FATAL => 'all';

use JSON::PP qw(decode_json encode_json);
use IO::Select;
use IO::Socket::UNIX;
use Socket qw(SOCK_STREAM);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('image_plan');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{CREATE EXTENSION semloom_pg VERSION '0.2.0';});
my $old_query = q{
SELECT oid, proname, proargtypes, provolatile, proparallel, proisstrict, prosecdef,
       coalesce(proacl::text, '') FROM pg_proc WHERE pronamespace='ai_semantic'::regnamespace
       AND proname <> 'embed' ORDER BY oid;
};
my $old = $node->safe_psql('postgres', $old_query);
$node->safe_psql('postgres', q{ALTER EXTENSION semloom_pg UPDATE TO '0.3.0';});
is($node->safe_psql('postgres', $old_query), $old, 'image upgrade preserves every old function identity and attribute');
is($node->safe_psql('postgres', q{
SELECT prorettype::regtype, provolatile, proparallel, proisstrict, prosecdef
FROM pg_proc WHERE oid='ai_semantic.embed(bytea,jsonb)'::regprocedure;
}), 'real[]|v|u|f|f', 'image marker has typed output, explicit NULL handling and ordinary caller privileges');
my $definition = $node->safe_psql('postgres', q{SELECT pg_get_functiondef('ai_semantic.embed(bytea,jsonb)'::regprocedure);});
$node->safe_psql('postgres', 'CREATE DATABASE image_fresh');
$node->safe_psql('image_fresh', q{CREATE EXTENSION semloom_pg VERSION '0.3.0';});
is($node->safe_psql('image_fresh', q{SELECT pg_get_functiondef('ai_semantic.embed(bytea,jsonb)'::regprocedure);}),
   $definition, 'fresh and upgraded image interfaces are identical');

my $options = encode_json({model_id=>'fixture/rgb-projection', model_revision=>('a' x 40),
    processor_id=>'fixture/pillow-rgb', processor_revision=>('b' x 40), dtype=>'float32', dimension=>3, input_size=>2});
my $query = "SELECT id, ai_semantic.embed(image, '$options'::jsonb) FROM ONLY image_inputs";
$node->safe_psql('postgres', q{CREATE TABLE image_inputs(id integer, image bytea); INSERT INTO image_inputs VALUES (1,NULL),(2,NULL);});
my $socket_path = $node->host . '/image-no-task.sock';
my $listener = IO::Socket::UNIX->new(Type=>SOCK_STREAM, Local=>$socket_path, Listen=>8)
    or die "cannot create image no-task sentinel";
my @digests;
my @physical;
for my $mode ('reference', 'staged')
{
    my $setup = "SET semloom_pg.provider_execution_profile='image-$mode'; SET semloom_pg.gateway_socket='$socket_path'; ";
    my $plan = decode_json($node->safe_psql('postgres', $setup . "EXPLAIN (FORMAT JSON) $query"))->[0]->{'Plan'};
    is($plan->{'Custom Plan Provider'}, 'SemLoom SemMap', "$mode reuses the Map carrier");
    is($plan->{'Semantic Plan Schema'}, 5, "$mode uses image schema five");
    is($plan->{'Image Output Dimension'}, 3, "$mode owns output dimension");
    is($plan->{'Plans'}->[0]->{'Node Type'}, 'Seq Scan', "$mode retains an ordinary PostgreSQL child");
    push @digests, $plan->{'Semantic Spec Digest'};
    push @physical, $plan->{'Physical Algorithm Digest'};
    is($node->safe_psql('postgres', $setup . $query), "1|\n2|", "$mode propagates NULL without a task");
    is($node->safe_psql('postgres', $setup . $query . ' LIMIT 0'), '', "$mode LIMIT zero returns no rows");
    is($node->safe_psql('postgres', $setup . "SET plan_cache_mode=force_generic_plan; PREPARE images AS $query; EXECUTE images;"),
       "1|\n2|", "$mode prepared plans retain typed identity");
}
is($digests[0], $digests[1], 'physical staging preserves image semantics');
is($digests[0], '0abea2e85fb800fa8c15706e55b9bd0ce85f1e0a19fb001f5e6e7de1f562f8a2',
   'PostgreSQL image identity matches the independent Python vector');
isnt($physical[0], $physical[1], 'reference and staged algorithms have separate identities');
ok(!IO::Select->new($listener)->can_read(0.1), 'EXPLAIN, NULL and LIMIT zero made no provider connection');
$listener->close;
$node->stop;
done_testing();
