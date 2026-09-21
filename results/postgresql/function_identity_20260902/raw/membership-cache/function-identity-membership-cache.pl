use strict;
use warnings FATAL => 'all';
use JSON::PP qw(encode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('membership_cache');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE inputs(content text);
INSERT INTO inputs VALUES ('false');
});
my $query = 'SELECT ai_semantic.map(content) FROM inputs';
my $session = $node->background_psql('postgres');
$session->query_safe("SET plan_cache_mode=force_generic_plan; PREPARE p AS $query");
my $before = $session->query_safe('EXPLAIN EXECUTE p');
$node->safe_psql('postgres', 'ALTER EXTENSION semloom_pg DROP FUNCTION ai_semantic.map(text)');
my $cached = $session->query_safe('EXPLAIN EXECUTE p');
my $fresh = $node->safe_psql('postgres', "EXPLAIN $query");
$session->query_safe('DISCARD PLANS');
my $discarded = $session->query_safe('EXPLAIN EXECUTE p');
print encode_json({
    pg_version => $node->safe_psql('postgres', 'SHOW server_version'),
    initial_plan => $before, cached_after_detach => $cached,
    fresh_after_detach => $fresh, cached_after_discard => $discarded,
}), "\n";
like($before, qr/Custom Scan/, 'initial plan uses the member');
unlike($fresh, qr/Custom Scan/, 'fresh plan does not lower the detached marker');
unlike($discarded, qr/Custom Scan/, 'discarding plans applies the current membership');
$session->quit;
$node->stop;
done_testing();
