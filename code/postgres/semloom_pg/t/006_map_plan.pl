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

my $test_dir = abs_path("$FindBin::RealBin/plan_contract");
command_ok(['make', '-s', '-C', $test_dir, 'COPT=-O2 -Werror'], 'build the production plan codec caller and callback observer');
my $node = PostgreSQL::Test::Cluster->new('map_plan');
$node->init;
my $socket_path = $node->host . '/map-never-connect.sock';
my $listener = IO::Socket::UNIX->new(Type => SOCK_STREAM, Local => $socket_path, Listen => 8)
  or die "could not create provider connection sentinel";
$node->append_conf('postgresql.conf', "shared_preload_libraries = '$test_dir/semloom_plan_contract_test,semloom_pg'\n");
$node->append_conf('postgresql.conf', "semloom_pg.gateway_socket = '$socket_path'\n");
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
    $node->safe_psql('postgres', q{
CREATE TABLE map_inputs(id integer, body text);
INSERT INTO map_inputs VALUES (1, 'hello'), (2, NULL);
});
    my $options = q|'{"model":"golden-map-v1","temperature":0,"max_tokens":128}'::jsonb|;
    my $query = "SELECT id, ai_semantic.map(body, 'Echo the input.', $options) AS generated FROM ONLY map_inputs";
    my $plan = decode_json($node->safe_psql('postgres', "EXPLAIN (FORMAT JSON) $query"))->[0]->{'Plan'};
    is($plan->{'Custom Plan Provider'}, 'SemLoom SemMap', 'generative Map uses the existing Map carrier');
    is($plan->{'Semantic Plan Schema'}, 4, 'new Map owns schema 4');
    is($plan->{'Semantic Spec'}, 'semloom.semantic.sem_map.generate.v1', 'plan identifies generative Map semantics');
    is($plan->{'Semantic Spec Digest'}, 'b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba',
       'PG plan identity matches the independent Map vector');
    is($plan->{'Prompt Program'}, 'semloom.sem_map.chat.v1', 'Map owns its prompt program');
    is($plan->{'Result Parser'}, 'semloom.sem_map.utf8_text.v1', 'Map owns its text result policy');
    is($plan->{'Model'}, 'golden-map-v1', 'plan owns the model');
    is($plan->{'Max Tokens'}, 128, 'plan owns the normalized output token cap');
    is($plan->{'Max Input Bytes'}, 163840, 'plan owns its input byte cap');
    is($plan->{'Max Output Bytes'}, 65536, 'plan owns its output byte cap');
    is($plan->{'Execution Support'}, 'plan-only', 'EXPLAIN does not claim connected execution');
    unlike($node->safe_psql('postgres', "EXPLAIN $query"), qr/Echo the input\./, 'EXPLAIN does not disclose the instruction');
    my ($code, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\n$query;");
    isnt($code, 0, 'unconnected generative Map cannot execute');
    like($err, qr/ERROR:  0A000: generative SemMap execution is not connected\n/,
         'new plan cannot silently use an old provider path');

    for my $mode ('force_custom_plan', 'force_generic_plan')
    {
        for my $parameter (
            ['instruction', 'text', '$1', $options, q|'Echo the input.'|],
            ['options', 'jsonb', q|'Echo the input.'|, '$1', $options])
        {
            my ($label, $type, $instruction, $config, $value) = @$parameter;
            my $sql = "SET plan_cache_mode=$mode; PREPARE map_parameter($type) AS " .
                "SELECT ai_semantic.map(body, $instruction, $config) FROM ONLY map_inputs; " .
                "EXPLAIN EXECUTE map_parameter($value);";
            my ($result, $output, $error) = $node->psql('postgres', "\\set VERBOSITY verbose\n$sql");
            isnt($result, 0, "$mode rejects a parameter used as $label");
            like($error, qr/ERROR:  0A000: SemMap instruction and options must be fixed immutable constants\n/,
                 "$mode reports the same early-source error for $label");
        }
        my $parameter_plan = decode_json($node->safe_psql('postgres',
            "SET plan_cache_mode=$mode; PREPARE map_input(text, integer) AS " .
            "SELECT ai_semantic.map(\$1, 'Echo the input.', $options) FROM ONLY map_inputs WHERE id >= \$2; " .
            "EXPLAIN (FORMAT JSON) EXECUTE map_input('hello', 1);"))->[0]->{'Plan'};
        is($parameter_plan->{'Semantic Spec Digest'}, $plan->{'Semantic Spec Digest'},
           "$mode still permits ordinary input and predicate parameters");
    }
    $node->safe_psql('postgres', q{
CREATE FUNCTION stable_instruction() RETURNS text LANGUAGE sql STABLE AS $$ SELECT 'Echo the input.'::text $$;
CREATE FUNCTION volatile_instruction() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT 'Echo the input.'::text $$;
});
    for my $instruction ('body', "(SELECT 'Echo the input.'::text)", 'stable_instruction()', 'volatile_instruction()')
    {
        my ($result, $output, $error) = $node->psql('postgres',
            "\\set VERBOSITY verbose\nEXPLAIN SELECT ai_semantic.map(body, $instruction, $options) FROM ONLY map_inputs;");
        isnt($result, 0, "fixed instruction rejects $instruction");
        like($error, qr/ERROR:  0A000: SemMap instruction and options must be fixed immutable constants\n/,
             'non-constant instruction has the exact early-source error');
    }
    my $folded = decode_json($node->safe_psql('postgres',
        "EXPLAIN (FORMAT JSON) SELECT ai_semantic.map(body, 'Echo ' || 'the input.', " .
        q|' {"max_tokens":128.0,"temperature":-0.0,"model":"golden-map-v1"}'::jsonb| .
        ') FROM ONLY map_inputs;'))->[0]->{'Plan'};
    is($folded->{'Semantic Spec Digest'}, $plan->{'Semantic Spec Digest'},
       'ordinary immutable folding and equal numeric values preserve Map identity');

    $node->safe_psql('postgres', q{
CREATE ROLE map_reader;
GRANT USAGE ON SCHEMA ai_semantic TO map_reader;
GRANT SELECT ON map_inputs TO map_reader;
REVOKE ALL ON FUNCTION ai_semantic.map(text,text,jsonb) FROM PUBLIC;
CREATE FUNCTION map_test_capture(statement text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE code text; message text;
BEGIN
  EXECUTE statement;
  RETURN 'unexpected success';
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS code = RETURNED_SQLSTATE, message = MESSAGE_TEXT;
  RETURN code || '|' || message;
END $$;
});
    my $reader = $node->background_psql('postgres');
    $reader->query_safe('SET ROLE map_reader');
    my $capture = sub {
        my ($sql) = @_;
        $sql =~ s/'/''/g;
        return $reader->query_safe("SELECT map_test_capture('$sql');");
    };
    for my $statement ($query, "$query LIMIT 0", "$query WHERE false", "$query WHERE body IS NULL", "EXPLAIN $query")
    {
        is($capture->($statement), '42501|permission denied for function map',
           'missing EXECUTE is checked even for plan-only, empty and NULL queries');
    }
    $node->safe_psql('postgres', 'GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO map_reader;');
    for my $mode ('force_custom_plan', 'force_generic_plan')
    {
        $reader->query_safe("SET plan_cache_mode=$mode; PREPARE map_acl(integer) AS $query WHERE id >= \$1;");
        my $saved = decode_json($reader->query_safe('EXPLAIN (FORMAT JSON) EXECUTE map_acl(1)'))->[0]->{'Plan'};
        is($saved->{'Semantic Spec Digest'}, $plan->{'Semantic Spec Digest'}, "$mode caches the complete Map definition");
        is($capture->('EXECUTE map_acl(1)'), '0A000|generative SemMap execution is not connected',
           "$mode authorized execution reaches the explicit unconnected state");
        my $counter = $mode eq 'force_custom_plan' ? 'custom_plans' : 'generic_plans';
        is($reader->query_safe("SELECT $counter > 0 FROM pg_prepared_statements WHERE name='map_acl'"), 't',
           "$mode actually exercised its requested plan kind before revocation");
        $node->safe_psql('postgres', 'REVOKE EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) FROM map_reader;');
        is($capture->('EXECUTE map_acl(1)'), '42501|permission denied for function map',
           "$mode rechecks EXECUTE after another session revokes it");
        $node->safe_psql('postgres', 'GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO map_reader;');
        is($capture->('EXECUTE map_acl(1)'), '0A000|generative SemMap execution is not connected',
           "$mode permission grant restores authorized plan initialization");
        $reader->query_safe('DEALLOCATE map_acl');
    }
    $reader->quit;

    $node->safe_psql('postgres', qq{
CREATE FUNCTION test_map_plan(text, oid) RETURNS text
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_map_plan' LANGUAGE C STRICT;
CREATE FUNCTION map_watch(oid, oid) RETURNS void
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_map_watch' LANGUAGE C STRICT;
CREATE FUNCTION map_events() RETURNS text
AS '$test_dir/semloom_plan_contract_test', 'semloom_test_map_events' LANGUAGE C;
CREATE FUNCTION map_child_probe(text) RETURNS text LANGUAGE plpgsql VOLATILE
AS \$\$ BEGIN RETURN \$1; END \$\$;
});
    my $marker_oid = $node->safe_psql('postgres', "SELECT 'ai_semantic.map(text,text,jsonb)'::regprocedure::oid");
    my $physical_digest = '558e50ae5e2716d2e699e09ddb8ffb953f772ba9a1be9dbb15379d9bfcf08d66';
    my $map_digest = 'b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba';
    my $suffix = "$map_digest|$physical_digest|128|163840|65536|absent|golden-map-v1|Echo the input.";
    is($node->safe_psql('postgres', "SELECT test_map_plan('copy', $marker_oid)"),
       "4|1|$marker_oid|$suffix", 'Map values survive deletion of both the source and copied plan contexts');
    is($node->safe_psql('postgres', "SELECT test_map_plan('column', $marker_oid)"),
       "4|2|$marker_oid|$suffix", 'Map column binding is outside semantic identity');
    is($node->safe_psql('postgres', "SELECT test_map_plan('copy', 4026531841::oid)"),
       "4|1|4026531841|$suffix", 'full-width private function OID survives copying without changing semantic identity');
    my %invalid_plans = (
        duplicate => 'duplicate semantic plan specification field',
        missing => 'incomplete semantic plan specification',
        unknown => 'unknown semantic plan specification field',
        map { $_ => 'invalid semantic executor binding' }
            qw(binding-type binding-column binding-function-type binding-function-null binding-function-zero binding-function-byref),
    );
    for my $field (qw(schema_version operator_kind input_value_kind output_value_kind null_policy error_policy
        semantic_spec_version semantic_spec_id physical_algorithm physical_role order_policy instruction prompt_program_id
        prompt_program_version prompt_program_digest result_parser_id result_parser_version result_parser_digest model_id
        temperature top_p max_tokens n stream has_stop max_input_bytes max_output_bytes semantic_spec_digest physical_algorithm_digest))
    {
        $invalid_plans{"field:$field"} = $field eq 'schema_version' ? 'unsupported semantic plan specification' :
            'unsupported generative SemMap plan specification';
    }
    for my $mutation (sort keys %invalid_plans)
    {
        my ($result, $output, $error) = $node->psql('postgres',
            "\\set VERBOSITY verbose\nSELECT test_map_plan('$mutation', $marker_oid);");
        isnt($result, 0, "strict Map decoding rejects $mutation");
        like($error, qr/ERROR:  XX000: \Q$invalid_plans{$mutation}\E\n/, "$mutation has the exact redacted decoder error");
    }

    my $child_oid = $node->safe_psql('postgres', "SELECT 'map_child_probe(text)'::regprocedure::oid");
    my $hooks = $node->background_psql('postgres');
    $hooks->query_safe("SELECT map_watch($marker_oid, $child_oid); SET plan_cache_mode=force_generic_plan; " .
        "PREPARE hook_map(integer) AS $query WHERE id >= \$1;");
    $hooks->query_safe('EXPLAIN EXECUTE hook_map(1)');
    $hooks->query_safe('EXPLAIN EXECUTE hook_map(1)');
    is($hooks->query_safe('SELECT map_events()'), '2|0|1',
       'cached Map calls the native execution hook once per initialization and chains the previous planner hook');
    $hooks->query_safe("SELECT map_watch($marker_oid, $child_oid); PREPARE hook_child AS " .
        "SELECT ai_semantic.map(map_child_probe(body), 'Echo the input.', $options) FROM ONLY map_inputs;");
    is($hooks->query_safe(q{SELECT map_test_capture('EXECUTE hook_child')}),
       '0A000|generative SemMap execution is not connected', 'unconnected execution stops before child initialization');
    is($hooks->query_safe('SELECT map_events()'), '1|0|1', 'child function initialization has not run when execution is refused');
    $node->safe_psql('postgres', 'REVOKE EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) FROM map_reader;');
    $hooks->query_safe("SET ROLE map_reader; SELECT map_watch($marker_oid, $child_oid)");
    is($hooks->query_safe(q{SELECT map_test_capture('EXPLAIN EXECUTE hook_child')}),
       '42501|permission denied for function map', 'permission is checked before even the explain-only child initializer');
    my @denied_events = split /\|/, $hooks->query_safe('SELECT map_events()');
    is($denied_events[0], 0, 'failed ACL does not invoke the Map execution hook');
    is($denied_events[1], 0, 'failed ACL does not initialize the ordinary child function');
    $hooks->quit;

    for my $unsupported (
        ['constant CASE', "SELECT CASE WHEN true THEN ai_semantic.map(body, 'Echo the input.', $options) ELSE '' END FROM ONLY map_inputs",
         'ai_semantic.map is only supported as a top-level output expression'],
        ['WHERE expression', "SELECT id FROM ONLY map_inputs WHERE ai_semantic.map(body, 'Echo the input.', $options) = 'hello'",
         'ai_semantic.map is only supported as a top-level output expression'],
        ['Map and Filter', "$query WHERE ai_semantic.filter(body)",
         'SemMap and SemFilter cannot be combined in the current capability'])
    {
        my ($label, $statement, $message) = @$unsupported;
        my ($result, $output, $error) = $node->psql('postgres', "\\set VERBOSITY verbose\nEXPLAIN $statement;");
        isnt($result, 0, "$label is not silently enabled by adding Map");
        like($error, qr/ERROR:  0A000: \Q$message\E\n/, "$label has an explicit unsupported-shape error");
    }
}
ok(!IO::Select->new($listener)->can_read(0), 'plan-only Map made zero provider connections');
close($listener);
unlink($socket_path) or die "could not remove provider connection sentinel";
$node->stop;
done_testing();
