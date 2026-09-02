use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use JSON::PP qw(decode_json);
use IO::Select;
use IO::Socket::UNIX;
use Socket qw(SOCK_STREAM);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('choice_plan');
$node->init;
my $socket_path = $node->host . '/choice-never-connect.sock';
my $listener = IO::Socket::UNIX->new(Type => SOCK_STREAM, Local => $socket_path, Listen => 8)
  or die "could not create test provider listener: $!";
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->append_conf('postgresql.conf', "semloom_pg.gateway_socket_path = '$socket_path'\n");
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

my $profile_digest = '941327729217db0ad438a8d0c945750485c6047834229aa40912b254d90a24f7';
# Independently encoded from the published plan field order, checked with OpenSSL.
my $old_digest = '9ec789eab10d6367b60895288fde154b384edeba1ac0fb603ade0b2424ff2fb9';
my $new_digest = '3624a95a096a8a6b9e838676ec8865315b1f49c27a0e9594cf67a5440792d6c5';
my $physical_digest = '558e50ae5e2716d2e699e09ddb8ffb953f772ba9a1be9dbb15379d9bfcf08d66';
my $plan = decode_json($node->safe_psql('postgres', "EXPLAIN (FORMAT JSON) $query"))->[0]->{'Plan'};
is($plan->{'Semantic Plan Schema'}, 3, 'explicit option selects schema 3');
is($plan->{'Semantic Spec Digest'}, $new_digest, 'PG hashes the complete independently encoded profile');
is($plan->{'Generation Profile Digest'}, $profile_digest, 'PG profile digest matches the independent vector');
is($plan->{'Generation Profile Version'}, 1, 'plan preserves profile version');
is_deeply($plan->{'Generation Choices'}, ['TRUE', 'FALSE', 'UNKNOWN'], 'plan preserves choice bytes and order');
is($plan->{'Generation Quality'}, 'unqualified', 'choice support does not claim semantic qualification');
is($plan->{'AI Cost Calibration'}, 'unavailable', 'choice does not claim calibration');
is($plan->{'Provider'}, 'unavailable (wire v4 required)', 'plan does not impersonate a v3 provider');

my $test_dir = abs_path("$FindBin::RealBin/plan_contract");
command_ok(['make', '-s', '-C', $test_dir, 'COPT=-O2 -Werror'], 'build test-only PG plan codec caller');
$node->safe_psql('postgres', qq{
CREATE FUNCTION test_choice_plan(text) RETURNS text
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_plan' LANGUAGE C STRICT;
});
is($node->safe_psql('postgres', "SELECT test_choice_plan('old')"),
   "2|1|$old_digest|$physical_digest|absent", 'old plan and digest survive copy and source-context deletion');
is($node->safe_psql('postgres', "SELECT test_choice_plan('copy')"),
   "3|1|$new_digest|$physical_digest|$profile_digest", 'complete choice plan survives both tree lifetimes');
is($node->safe_psql('postgres', "SELECT test_choice_plan('binding')"),
   "3|2|$new_digest|$physical_digest|$profile_digest", 'column binding does not change semantic identity');

my %invalid_plans = (
    'missing-profile' => 'incomplete semantic plan specification',
    'old-with-profile' => 'incomplete semantic plan specification',
    'future-schema' => 'unsupported semantic plan specification',
    'outer-digest' => 'unsupported exact SemFilter plan specification',
    extra => 'invalid generation profile fields',
    missing => 'invalid generation profile fields',
    duplicate => 'duplicate semantic plan specification field',
    'unknown-field' => 'unknown generation profile field',
    'unknown-id' => 'unsupported generation profile',
    'oversized-id' => 'invalid semantic plan specification field',
    'unknown-version' => 'unsupported generation profile',
    'version-type' => 'invalid semantic plan specification field',
    constraint => 'unsupported generation profile constraint',
    digest => 'generation profile digest mismatch',
    'choices-type' => 'invalid generation profile choices',
    'choice-type' => 'invalid semantic plan specification field',
    'choice-content' => 'unsupported generation profile',
    'choice-order' => 'unsupported generation profile',
    'choice-count' => 'invalid generation profile choices',
    'binding-overflow' => 'invalid semantic executor binding',
    'null-profile' => 'invalid generation profile fields',
);
for my $mutation (sort keys %invalid_plans)
{
    my ($code, $out, $err) = $node->psql('postgres',
        "\\set VERBOSITY verbose\nSELECT test_choice_plan('$mutation');");
    isnt($code, 0, "strict plan decode rejects $mutation");
    like($err, qr/ERROR:  XX000: \Q$invalid_plans{$mutation}\E\n/,
         "$mutation has the exact redacted error contract");
}

my $old_options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb|;
my $old_query = "SELECT id FROM choice_inputs WHERE ai_semantic.filter(content, 'Classify input.', $old_options)";
my $old_plan = decode_json($node->safe_psql('postgres', "EXPLAIN (FORMAT JSON) $old_query"))->[0]->{'Plan'};
ok(!exists $old_plan->{'Generation Profile'}, 'old options do not acquire a profile');
is($old_plan->{'Provider'}, 'uds-golden', 'old options retain their provider identity');

for my $value ('null', '1', 'true', '[]', '{}', '"unknown"', '"semloom.generation.choice.tristate.v2"')
{
    my $invalid = qq|' {"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":$value}'::jsonb|;
    my ($code, $out, $err) = $node->psql('postgres',
        "\\set VERBOSITY verbose\nEXPLAIN SELECT id FROM choice_inputs WHERE ai_semantic.filter(content, 'Classify input.', $invalid);");
    isnt($code, 0, "planning rejects profile selector $value");
    like($err, qr/ERROR:  22023: unsupported SemFilter generation_profile\n/, 'selector error is exact and redacted');
}
for my $invalid (
    q|'{"model":"golden-model-v1","temperature":0,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|,
    q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1","extra":1}'::jsonb|)
{
    my ($code, $out, $err) = $node->psql('postgres',
        "\\set VERBOSITY verbose\nEXPLAIN SELECT id FROM choice_inputs WHERE ai_semantic.filter(content, 'Classify input.', $invalid);");
    isnt($code, 0, 'choice options reject missing or additional fields');
    like($err, qr/ERROR:  22023: SemFilter options must contain exactly model, temperature, and max_tokens\n/,
         'invalid field set retains the established error');
}

for my $statement ($query, "$query LIMIT 0", "$query AND id < 0", "$query AND content IS NULL", "EXPLAIN ANALYZE $query")
{
    my ($code, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\n$statement;");
    isnt($code, 0, 'choice execution cannot fall back, including no-task queries');
    like($err, qr/ERROR:  0A000: SemFilter choice execution requires wire v4, which is not implemented\n/,
         'choice execution has an explicit stable unsupported error');
}

my $prepared = $node->background_psql('postgres');
$prepared->query_safe("SET plan_cache_mode = force_generic_plan; PREPARE choice_p(integer) AS $query AND id >= \$1;");
for my $change ('', "SET semloom_pg.provider_execution_profile = 'openai-compatible-fixed';",
                'ALTER TABLE choice_inputs ADD COLUMN extra integer;')
{
    $prepared->query_safe($change) if $change ne '';
    my $saved = decode_json($prepared->query_safe('EXPLAIN (FORMAT JSON) EXECUTE choice_p(1)'))->[0]->{'Plan'};
    is($saved->{'Semantic Spec Digest'}, $new_digest, 'generic plan retains choice identity across GUC change and invalidation');
    is_deeply($saved->{'Generation Choices'}, ['TRUE', 'FALSE', 'UNKNOWN'], 'generic plan retains the complete ordered profile');
}
$prepared->quit;
my ($prepared_code, $prepared_out, $prepared_err) = $node->psql('postgres',
    "\\set VERBOSITY verbose\nSET plan_cache_mode=force_generic_plan; PREPARE blocked AS $query; EXECUTE blocked;");
isnt($prepared_code, 0, 'prepared choice execution is also blocked');
like($prepared_err, qr/ERROR:  0A000: SemFilter choice execution requires wire v4, which is not implemented\n/,
     'prepared execution never uses v3');
is($node->safe_psql('postgres', 'SELECT count(*) FROM choice_inputs'), 2, 'ordinary SQL remains usable after rejected execution');
ok(!IO::Select->new($listener)->can_read(0), 'all planning and rejected execution made zero provider connections');
close($listener);
unlink($socket_path) or die "could not remove test listener";
$node->stop;
done_testing();
