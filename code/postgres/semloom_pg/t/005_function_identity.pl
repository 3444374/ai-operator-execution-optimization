use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('function_identity');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE SCHEMA ai_semantic;
CREATE FUNCTION ai_semantic.map(text) RETURNS text LANGUAGE plpgsql VOLATILE
AS $$ BEGIN RETURN 'ordinary:' || $1; END $$;
CREATE TABLE identity_inputs(content text);
INSERT INTO identity_inputs VALUES ('false');
});
is($node->safe_psql('postgres', 'SELECT ai_semantic.map(content) FROM identity_inputs'),
   'ordinary:false', 'same-signature function without the extension keeps its own implementation');
unlike($node->safe_psql('postgres', 'EXPLAIN SELECT ai_semantic.map(content) FROM identity_inputs'),
       qr/Custom Scan/, 'non-member function is not lowered to a semantic scan');
$node->stop;
done_testing();
