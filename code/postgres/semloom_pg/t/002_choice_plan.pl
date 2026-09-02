use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('choice_plan');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE choice_inputs(id integer, content text);
INSERT INTO choice_inputs VALUES (1, 'database'), (2, NULL);
});
my $options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|;
my $query = "SELECT id FROM choice_inputs WHERE ai_semantic.filter(content, 'Classify input.', $options)";
my ($status, $stdout, $stderr) = $node->psql('postgres', "EXPLAIN $query");
is($status, 0, 'explicit choice profile can be planned without a provider') or diag($stderr);
like($stdout, qr/Generation Profile: semloom\.generation\.choice\.tristate/, 'EXPLAIN identifies the saved profile');
$node->stop;
done_testing();
