use strict;
use warnings FATAL => 'all';
use Cwd qw(abs_path);
use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $test_dir = abs_path("$FindBin::RealBin/plan_contract");
command_ok(['make', '-s', '-C', $test_dir, 'COPT=-O2 -Werror'], 'build production binding callers');
my $node = PostgreSQL::Test::Cluster->new('binding');
$node->init;
$node->start;
$node->safe_psql('postgres', qq{
CREATE FUNCTION test_binding(text) RETURNS boolean
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_binding' LANGUAGE C STRICT;
CREATE FUNCTION test_call() RETURNS boolean
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_call' LANGUAGE C;
CREATE FUNCTION test_binding_setrefs(oid) RETURNS boolean
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_binding_setrefs' LANGUAGE C STRICT;
CREATE EXTENSION semloom_pg;
});
is($node->safe_psql('postgres', q{SELECT test_binding_setrefs('ai_semantic.map(text,text,jsonb)'::regprocedure)}),
   't', 'setrefs maps matching marker to independent result Var and retains plan dependency');
is($node->safe_psql('postgres', 'SELECT test_call()'), 't',
   'equal occurrences own independent copies and distinct planner-local keys');
for my $mode ('map', 'binary', 'null', 'reuse', 'legacy-map', 'legacy-filter')
{
    is($node->safe_psql('postgres', "SELECT test_binding('$mode')"), 't',
       "$mode preserves input and routes results after source-plan memory is freed");
}
for my $mode ('input-range', 'input-overflow', 'input-type', 'result-range', 'result-overlap',
              'duplicate-target', 'missing-target', 'malformed-pair', 'malformed-list',
              'unknown-field', 'type-mismatch', 'typmod-mismatch', 'collation-mismatch',
              'dropped-input', 'dropped-result')
{
    my ($status, $out, $err) = $node->psql('postgres',
        "\\set VERBOSITY verbose\nSELECT test_binding('$mode');");
    isnt($status, 0, "$mode rejected");
    like($err, qr/ERROR:  XX000: invalid semantic tuple binding\n/, "$mode has bounded deterministic error");
}
is($node->safe_psql('postgres', "SELECT test_binding('map')"), 't', 'legal binding works after errors');
$node->stop;
done_testing();
