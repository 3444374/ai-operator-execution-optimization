use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('filter_insert');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE filter_source(id integer, decision text);
INSERT INTO filter_source VALUES
    (1,'true'), (2,'false'), (3,'unknown'), (4,NULL), (5,'true');
CREATE TABLE filter_sink(id integer);
});
my $insert = q{INSERT INTO filter_sink
SELECT id + 10 FROM filter_source WHERE id > 1 AND ai_semantic.filter(decision)};
my $plan = $node->safe_psql('postgres', "EXPLAIN (COSTS OFF) $insert");
like($plan, qr/Insert on filter_sink.*Custom Scan \(SemLoom SemFilter\)/s,
    'INSERT plans Filter below the PostgreSQL write node');
my ($status, $stdout, $stderr) = $node->psql('postgres', $insert);
is($status, 0, 'direct Filter INSERT executes') or diag($stderr);
is($node->safe_psql('postgres', 'SELECT id FROM filter_sink ORDER BY id'), '15',
    'ordinary predicate, projection and tristate Filter select the inserted row');
$node->stop;
done_testing();
