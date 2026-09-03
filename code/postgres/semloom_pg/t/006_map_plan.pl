use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('map_plan');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{CREATE EXTENSION semloom_pg VERSION '0.1.0';});
my $old_functions = $node->safe_psql('postgres', q{
SELECT oid, proname, proargtypes, provolatile, proparallel, proisstrict, prosecdef, proleakproof,
       coalesce(proacl::text, '')
FROM pg_proc WHERE pronamespace = 'ai_semantic'::regnamespace ORDER BY oid;
});
my ($status, $stdout, $stderr) = $node->psql('postgres', q{ALTER EXTENSION semloom_pg UPDATE TO '0.2.0';});
is($status, 0, 'existing extension upgrades to the generative Map SQL interface') or diag($stderr);
if ($status == 0)
{
    is($node->safe_psql('postgres', q{
SELECT oid, proname, proargtypes, provolatile, proparallel, proisstrict, prosecdef, proleakproof,
       coalesce(proacl::text, '')
FROM pg_proc WHERE pronamespace = 'ai_semantic'::regnamespace
AND oid <> 'ai_semantic.map(text,text,jsonb)'::regprocedure ORDER BY oid;
}), $old_functions, 'upgrade preserves all old function identities, attributes and grants');
    is($node->safe_psql('postgres', q{
SELECT prorettype::regtype, l.lanname, provolatile, proparallel, proisstrict, prosecdef, proleakproof
FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang
WHERE p.oid = 'ai_semantic.map(text,text,jsonb)'::regprocedure;
}), 'text|c|v|u|f|f|f', 'new Map has text output and explicit safe marker attributes');
}
$node->stop;
done_testing();
