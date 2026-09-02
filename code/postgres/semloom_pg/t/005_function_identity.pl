use strict;
use warnings FATAL => 'all';

use JSON::PP qw(encode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('function_identity');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->append_conf('postgresql.conf', "statement_timeout = '5s'\n");
$node->start;
my $legacy_options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8}'::jsonb|;
my $choice_options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|;
my @calls = (
    ['map', 'map(text)', 'text', q{'ordinary:' || $1},
     'SELECT ai_semantic.map(content) FROM identity_inputs', 'ordinary:false'],
    ['recording filter', 'filter(text)', 'boolean', 'true',
     'SELECT content FROM identity_inputs WHERE ai_semantic.filter(content)', 'false'],
    ['exact filter', 'filter(text,text,jsonb)', 'boolean', 'true',
     "SELECT content FROM identity_inputs WHERE ai_semantic.filter(content, 'Classify input.', $legacy_options)", 'false'],
    ['choice filter', 'filter(text,text,jsonb)', 'boolean', 'true',
     "SELECT content FROM identity_inputs WHERE ai_semantic.filter(content, 'Classify input.', $choice_options)", 'false'],
);

sub ordinary_definition
{
    my ($call) = @_;
    my $declaration = $call->[1];
    $declaration =~ s/\(text\)/(input text)/;
    $declaration =~ s/\(text,text,jsonb\)/(input text, instruction text, options jsonb)/;
    return "CREATE OR REPLACE FUNCTION ai_semantic.$declaration RETURNS $call->[2] " .
           'LANGUAGE plpgsql VOLATILE AS $$ BEGIN RETURN ' . $call->[3] . '; END $$;';
}

sub check_ordinary_calls
{
    my ($label) = @_;
    for my $call (@calls)
    {
        is($node->safe_psql('postgres', $call->[4]), $call->[5],
           "$label: $call->[0] keeps the ordinary implementation");
        unlike($node->safe_psql('postgres', "EXPLAIN $call->[4]"), qr/Custom Scan/,
               "$label: $call->[0] is not lowered");
    }
}

$node->safe_psql('postgres', 'CREATE SCHEMA ai_semantic');
$node->safe_psql('postgres', ordinary_definition($_)) for @calls;
$node->safe_psql('postgres', q{
CREATE TABLE identity_inputs(content text);
INSERT INTO identity_inputs VALUES ('false');
});
check_ordinary_calls('extension absent');
$node->safe_psql('postgres', 'DROP SCHEMA ai_semantic CASCADE; CREATE EXTENSION semloom_pg;');

for my $call (@calls)
{
    like($node->safe_psql('postgres', "EXPLAIN $call->[4]"), qr/Custom Scan \(SemLoom Sem/,
         "extension member: $call->[0] is lowered");
}
is($node->safe_psql('postgres', $calls[0]->[4]), 'recorded:false', 'member Map keeps recording behavior');
is($node->safe_psql('postgres', $calls[1]->[4]), '', 'member Filter keeps FALSE/drop behavior');

$node->safe_psql('postgres', q{
CREATE SCHEMA ordinary;
CREATE FUNCTION ordinary.map(text) RETURNS text LANGUAGE plpgsql VOLATILE
AS $$ BEGIN RETURN 'other:' || $1; END $$;
CREATE FUNCTION ordinary.filter(text) RETURNS boolean LANGUAGE plpgsql VOLATILE
AS $$ BEGIN RETURN true; END $$;
CREATE FUNCTION ai_semantic.map(integer) RETURNS text LANGUAGE plpgsql VOLATILE
AS $$ BEGIN RETURN 'integer:' || $1; END $$;
CREATE FUNCTION ai_semantic.filter(integer) RETURNS boolean LANGUAGE plpgsql VOLATILE
AS $$ BEGIN RETURN true; END $$;
});
for my $example (
    ['other schema Map', 'SELECT ordinary.map(content) FROM identity_inputs', 'other:false'],
    ['other schema Filter', 'SELECT content FROM identity_inputs WHERE ordinary.filter(content)', 'false'],
    ['overloaded Map', 'SELECT ai_semantic.map(length(content)) FROM identity_inputs', 'integer:5'],
    ['overloaded Filter', 'SELECT content FROM identity_inputs WHERE ai_semantic.filter(length(content))', 'false'])
{
    is($node->safe_psql('postgres', $example->[1]), $example->[2], "$example->[0] keeps its result");
    unlike($node->safe_psql('postgres', "EXPLAIN $example->[1]"), qr/Custom Scan/,
           "$example->[0] is not lowered");
}

for my $call (@calls)
{
    my ($label, $signature, undef, undef, $query, $expected) = @$call;
    my $qualified = "ai_semantic.$signature";
    my $original_oid = $node->safe_psql('postgres', "SELECT '$qualified'::regprocedure::oid");
    my $marker_definition = $node->safe_psql('postgres', "SELECT pg_get_functiondef('$qualified'::regprocedure)");
    my $prepared = $node->background_psql('postgres');
    $prepared->query_safe("SET plan_cache_mode=force_generic_plan; PREPARE identity_plan AS $query;");
    like($prepared->query_safe('EXPLAIN EXECUTE identity_plan'), qr/Custom Scan \(SemLoom Sem/,
         "$label: generic plan initially uses the extension member");

    $node->safe_psql('postgres', "ALTER EXTENSION semloom_pg DROP FUNCTION $qualified;");
    unlike($node->safe_psql('postgres', "EXPLAIN $query"), qr/Custom Scan/,
           "$label: fresh planning rejects a detached marker");
    $node->safe_psql('postgres', ordinary_definition($call));
    is($node->safe_psql('postgres', "SELECT '$qualified'::regprocedure::oid"), $original_oid,
       "$label: replacing the detached function keeps its OID");
    is($prepared->query_safe('EXECUTE identity_plan'), $expected,
       "$label: same-OID replacement invalidates the prepared semantic plan");
    unlike($prepared->query_safe('EXPLAIN EXECUTE identity_plan'), qr/Custom Scan/,
           "$label: prepared plan now uses the ordinary function");

    $node->safe_psql('postgres', "ALTER EXTENSION plpgsql ADD FUNCTION $qualified;");
    unlike($node->safe_psql('postgres', "EXPLAIN $query"), qr/Custom Scan/,
           "$label: membership in another extension does not qualify");
    $node->safe_psql('postgres', "ALTER EXTENSION plpgsql DROP FUNCTION $qualified; DROP FUNCTION $qualified;" .
                     ordinary_definition($call));
    isnt($node->safe_psql('postgres', "SELECT '$qualified'::regprocedure::oid"), $original_oid,
          "$label: drop and recreate gives the function a new OID");
    is($prepared->query_safe('EXECUTE identity_plan'), $expected,
       "$label: prepared plan resolves the recreated ordinary function");
    unlike($prepared->query_safe('EXPLAIN EXECUTE identity_plan'), qr/Custom Scan/,
           "$label: recreated non-member is not lowered");

    $node->safe_psql('postgres', "DROP FUNCTION $qualified; $marker_definition; " .
                     "ALTER EXTENSION semloom_pg ADD FUNCTION $qualified;");
    like($prepared->query_safe('EXPLAIN EXECUTE identity_plan'), qr/Custom Scan \(SemLoom Sem/,
         "$label: recreated extension member is lowered after revalidation");
    $prepared->quit;
}

for my $call (@calls)
{
    my ($label, $signature, undef, undef, $query) = @$call;
    my $qualified = "ai_semantic.$signature";
    my $reader = $node->background_psql('postgres');
    my $ddl = $node->background_psql('postgres');
    isnt($reader->query_safe('SELECT pg_backend_pid()'), $ddl->query_safe('SELECT pg_backend_pid()'),
          "$label: membership test uses two physical connections");
    my $definition_query = "SELECT '$qualified'::regprocedure::oid, pg_get_functiondef('$qualified'::regprocedure)";
    my $original = $ddl->query_safe($definition_query);
    $reader->query_safe("SET plan_cache_mode=force_generic_plan; PREPARE membership_plan AS $query;");
    like($reader->query_safe('EXPLAIN EXECUTE membership_plan'), qr/Custom Scan \(SemLoom Sem/,
         "$label: reader caches the member plan before DDL");

    for my $action ('DROP', 'ADD')
    {
        $ddl->query_safe("BEGIN; ALTER EXTENSION semloom_pg $action FUNCTION $qualified; COMMIT;");
        is($ddl->query_safe($definition_query), $original,
           "$label: $action changes membership without changing function identity or body");
        my $before_refresh = $reader->query_safe('EXPLAIN EXECUTE membership_plan');
        $reader->query_safe('DISCARD PLANS');
        my $after_refresh = $reader->query_safe('EXPLAIN EXECUTE membership_plan');
        note(encode_json({operator => $label, membership_action => $action,
                          before_refresh => $before_refresh, after_refresh => $after_refresh}));
        if ($action eq 'DROP')
        {
            unlike($after_refresh, qr/Custom Scan/,
                   "$label: reader refresh stops lowering the detached marker");
        }
        else
        {
            like($after_refresh, qr/Custom Scan \(SemLoom Sem/,
                 "$label: reader refresh resumes lowering the reattached marker");
        }
        is($reader->query_safe(q{SELECT count(*) FROM pg_prepared_statements WHERE name='membership_plan'}),
           1, "$label: $action refresh preserves the prepared statement for replanning");
    }
    $reader->quit;
    $ddl->quit;
}

$node->safe_psql('postgres', 'DROP EXTENSION semloom_pg CASCADE; CREATE SCHEMA ai_semantic;');
$node->safe_psql('postgres', ordinary_definition($_)) for @calls;
check_ordinary_calls('extension dropped');
is($node->safe_psql('postgres', 'SELECT content FROM identity_inputs'), 'false', 'ordinary SQL remains usable');
$node->stop;
done_testing();
