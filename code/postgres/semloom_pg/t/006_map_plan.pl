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
$node->safe_psql('postgres', q{
CREATE ROLE old_map_reader;
REVOKE EXECUTE ON FUNCTION ai_semantic.map(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ai_semantic.map(text) TO old_map_reader;
});
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
    my $definition_query = q{
SELECT pg_get_functiondef('ai_semantic.map(text,text,jsonb)'::regprocedure);
};
    my $map_definition = $node->safe_psql('postgres', $definition_query);
    $node->safe_psql('postgres', 'CREATE DATABASE map_fresh');
    $node->safe_psql('map_fresh', 'CREATE EXTENSION semloom_pg');
    is($node->safe_psql('map_fresh', "SELECT extversion FROM pg_extension WHERE extname='semloom_pg'"),
       '0.2.0', 'fresh installation selects the new extension version');
    is($node->safe_psql('map_fresh', $definition_query), $map_definition,
       'fresh and upgraded installations expose the same new SQL definition');
    for my $database ('postgres', 'map_fresh')
    {
        is($node->safe_psql($database, q{
SELECT e.extname FROM pg_depend d JOIN pg_extension e ON e.oid = d.refobjid
WHERE d.classid = 'pg_proc'::regclass AND d.objid = 'ai_semantic.map(text,text,jsonb)'::regprocedure
AND d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e';
}), 'semloom_pg', "$database: the generated Map function belongs to the expected extension");
    }
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
    is($plan->{'Provider'}, 'uds-golden', 'EXPLAIN selects the semantic adapter without opening it');
    unlike($node->safe_psql('postgres', "EXPLAIN $query"), qr/Echo the input\./, 'EXPLAIN does not disclose the instruction');
    my ($code, $out, $err) = $node->psql('postgres', "\\set VERBOSITY verbose\nSET semloom_pg.gateway_socket='invalid-relative-path'; $query;");
    isnt($code, 0, 'generative Map rejects an invalid provider path');
    like($err, qr/ERROR:  22023: SemLoom provider socket path must be absolute\n/,
         'new plan cannot silently use an old provider path');

    $node->safe_psql('postgres', q{
CREATE FUNCTION wrapped_map(input text, instruction text, options jsonb) RETURNS text
LANGUAGE sql VOLATILE RETURN ai_semantic.map(input, instruction, options);
CREATE FUNCTION wrapped_map_text(input text, instruction text, options jsonb) RETURNS text
LANGUAGE sql VOLATILE AS $$ SELECT ai_semantic.map($1, $2, $3) $$;
CREATE FUNCTION ordinary_echo(text) RETURNS text LANGUAGE sql IMMUTABLE RETURN $1 || '';
});
    for my $mode ('force_custom_plan', 'force_generic_plan')
    {
      for my $wrapper ('wrapped_map', 'wrapped_map_text')
      {
        my ($result, $output, $error) = $node->psql('postgres',
            "\\set VERBOSITY verbose\nSET plan_cache_mode=$mode; PREPARE wrapped_map_parameter(text) AS " .
            "SELECT $wrapper(body, \$1, $options) FROM ONLY map_inputs; " .
            "EXPLAIN EXECUTE wrapped_map_parameter('Echo the input.');");
        isnt($result, 0, "$mode rejects a prepared instruction inside $wrapper");
        like($error, qr/ERROR:  0A000: generative SemMap must be a direct query output\n/,
             "$mode rejects the wrapped source before execution");
        unlike($output, qr/Custom Scan \(SemLoom SemMap\)/, 'wrapped parameter cannot acquire a generative Map plan');
        ok(!IO::Select->new($listener)->can_read(0), "$mode wrapper check made zero provider connections");
      }
      my $ordinary_plan = $node->safe_psql('postgres',
          "SET plan_cache_mode=$mode; PREPARE ordinary_input(text) AS " .
          'SELECT ordinary_echo($1) FROM ONLY map_inputs; EXPLAIN (VERBOSE) EXECUTE ordinary_input(\'hello\');');
      unlike($ordinary_plan, qr/ordinary_echo/, "$mode keeps ordinary SQL function inlining");
      my $map_input = decode_json($node->safe_psql('postgres',
          "SET plan_cache_mode=$mode; PREPARE direct_map_input(text) AS " .
          "SELECT ai_semantic.map(ordinary_echo(\$1), 'Echo the input.', $options) FROM ONLY map_inputs; " .
          "EXPLAIN (FORMAT JSON) EXECUTE direct_map_input('hello');"))->[0]->{'Plan'};
      is($map_input->{'Semantic Spec Digest'}, $plan->{'Semantic Spec Digest'},
         "$mode permits an ordinary input wrapper beneath a direct Map");
    }
    my ($wrapped_code, $wrapped_out, $wrapped_err) = $node->psql('postgres',
        "\\set VERBOSITY verbose\nEXPLAIN SELECT wrapped_map(body, 'Echo the input.', $options) FROM ONLY map_inputs;");
    isnt($wrapped_code, 0, 'a literal wrapper does not implicitly add a new Map SQL entry');
    like($wrapped_err, qr/ERROR:  0A000: generative SemMap must be a direct query output\n/,
         'wrapper rejection does not depend on parameter values');
    $node->safe_psql('postgres', 'DROP FUNCTION wrapped_map(text,text,jsonb); DROP FUNCTION wrapped_map_text(text,text,jsonb);');
    $node->safe_psql('postgres', q{
CREATE FUNCTION nested_instruction() RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result text;
BEGIN
  BEGIN
    EXECUTE 'SELECT 1 / 0';
  EXCEPTION WHEN division_by_zero THEN
    NULL;
  END;
  EXECUTE 'SELECT ''Echo the input.''::text' INTO result;
  RETURN result;
END $$;
});
    my $nested_plan = decode_json($node->safe_psql('postgres',
        "EXPLAIN (FORMAT JSON) SELECT ai_semantic.map(body, nested_instruction(), $options) FROM ONLY map_inputs;"))->[0]->{'Plan'};
    is($nested_plan->{'Semantic Spec Digest'}, $plan->{'Semantic Spec Digest'},
       'nested planning success and caught ERROR restore the outer source check');

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

    for my $invalid (
        ['NULL instruction', 'NULL::text', $options, 'SemMap instruction and options must be non-NULL text and jsonb constants'],
        ['NULL options', q|'Echo the input.'|, 'NULL::jsonb', 'SemMap instruction and options must be non-NULL text and jsonb constants'],
        ['empty instruction', q|''|, $options, 'SemMap instruction must contain 1 to 4096 UTF8 bytes'],
        ['long instruction', q|repeat('x',4097)|, $options, 'SemMap instruction must contain 1 to 4096 UTF8 bytes'],
        ['long Unicode instruction', q|repeat(chr(30028),1366)|, $options, 'SemMap instruction must contain 1 to 4096 UTF8 bytes'])
    {
        my ($label, $instruction, $config, $message) = @$invalid;
        my ($result, $output, $error) = $node->psql('postgres',
            "\\set VERBOSITY verbose\nEXPLAIN SELECT ai_semantic.map(body, $instruction, $config) FROM ONLY map_inputs;");
        isnt($result, 0, "$label is rejected at planning");
        like($error, qr/ERROR:  22023: \Q$message\E\n/, "$label returns its bounded SQL validation error");
    }
    for my $instruction (q{repeat('x',4096)}, q{repeat(chr(30028),1365) || 'x'}, q{' '})
    {
        my $accepted = decode_json($node->safe_psql('postgres',
            "EXPLAIN (FORMAT JSON) SELECT ai_semantic.map(body, $instruction, $options) FROM ONLY map_inputs"))->[0]->{'Plan'};
        is($accepted->{'Semantic Plan Schema'}, 4, 'instruction boundary counts UTF8 bytes without trimming');
    }
    for my $option_case (
        ['JSON null', 'null', 'SemMap options must contain exactly model, temperature, and max_tokens'],
        ['array', '[]', 'SemMap options must contain exactly model, temperature, and max_tokens'],
        ['missing field', '{"model":"golden-map-v1","temperature":0}', 'SemMap options must contain exactly model, temperature, and max_tokens'],
        ['extra field', '{"model":"golden-map-v1","temperature":0,"max_tokens":128,"stop":[]}', 'SemMap options must contain exactly model, temperature, and max_tokens'],
        ['wrong field', '{"model":"golden-map-v1","temperature":0,"tokens":128}', 'SemMap options must contain exactly model, temperature, and max_tokens'],
        ['empty model', '{"model":"","temperature":0,"max_tokens":128}', 'SemMap model must contain 1 to 128 UTF8 bytes'],
        ['NULL model', '{"model":null,"temperature":0,"max_tokens":128}', 'SemMap model must contain 1 to 128 UTF8 bytes'],
        ['numeric model', '{"model":7,"temperature":0,"max_tokens":128}', 'SemMap model must contain 1 to 128 UTF8 bytes'],
        ['nonzero temperature', '{"model":"golden-map-v1","temperature":0.1,"max_tokens":128}', 'SemMap temperature must be numeric zero'],
        ['string temperature', '{"model":"golden-map-v1","temperature":"0","max_tokens":128}', 'SemMap temperature must be numeric zero'],
        map { ["token cap $_", '{"model":"golden-map-v1","temperature":0,"max_tokens":' . $_ . '}',
               'SemMap max_tokens must be an integer from 1 to 4096'] } qw(0 -1 4097 1.5 1e30 null true "128"))
    {
        my ($label, $config, $message) = @$option_case;
        my ($result, $output, $error) = $node->psql('postgres',
            "\\set VERBOSITY verbose\nEXPLAIN SELECT ai_semantic.map(body, 'Echo the input.', '$config'::jsonb) FROM ONLY map_inputs;");
        isnt($result, 0, "$label is rejected in Map options");
        like($error, qr/ERROR:  22023: \Q$message\E\n/, "$label has an exact non-payload option error");
    }
    for my $tokens (1, 4096)
    {
        my $accepted = decode_json($node->safe_psql('postgres',
            "EXPLAIN (FORMAT JSON) SELECT ai_semantic.map(body, 'Echo the input.', " .
            "'{\"model\":\"golden-map-v1\",\"temperature\":0,\"max_tokens\":$tokens}'::jsonb) FROM ONLY map_inputs"))->[0]->{'Plan'};
        is($accepted->{'Max Tokens'}, $tokens, 'Map accepts its own output cap boundary independently of Filter');
    }
    my $canonical = decode_json($node->safe_psql('postgres',
        "EXPLAIN (FORMAT JSON) SELECT ai_semantic.map(body, 'Echo the input.', " .
        q|' {"model":"unused","model":"golden-map-v1","temperature":0e10,"max_tokens":1.28e2}'::jsonb| .
        ') FROM ONLY map_inputs;'))->[0]->{'Plan'};
    is($canonical->{'Semantic Spec Digest'}, $plan->{'Semantic Spec Digest'},
       'JSONB last-key semantics and equivalent exponent numbers normalize before plan identity');

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
           'missing EXECUTE is checked even for EXPLAIN, empty and NULL queries');
        ok(!IO::Select->new($listener)->can_read(0), 'denied Map does not connect to its configured provider');
    }
    $node->safe_psql('postgres', 'GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO map_reader;');
    for my $mode ('force_custom_plan', 'force_generic_plan')
    {
        $reader->query_safe("SET plan_cache_mode=$mode; PREPARE map_acl(integer) AS $query WHERE id >= \$1;");
        my $saved = decode_json($reader->query_safe('EXPLAIN (FORMAT JSON) EXECUTE map_acl(1)'))->[0]->{'Plan'};
        is($saved->{'Semantic Spec Digest'}, $plan->{'Semantic Spec Digest'}, "$mode caches the complete Map definition");
        $reader->query_safe("SET semloom_pg.gateway_socket='invalid-relative-path'");
        is($capture->('EXECUTE map_acl(1)'), '22023|SemLoom provider socket path must be absolute',
           "$mode authorized execution reaches lazy provider validation");
        my $counter = $mode eq 'force_custom_plan' ? 'custom_plans' : 'generic_plans';
        is($reader->query_safe("SELECT $counter > 0 FROM pg_prepared_statements WHERE name='map_acl'"), 't',
           "$mode actually exercised its requested plan kind before revocation");
        $node->safe_psql('postgres', 'REVOKE EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) FROM map_reader;');
        $reader->query_safe("SET semloom_pg.gateway_socket='$socket_path'");
        is($capture->('EXECUTE map_acl(1)'), '42501|permission denied for function map',
           "$mode rechecks EXECUTE after another session revokes it");
        ok(!IO::Select->new($listener)->can_read(0), 'cached revoked Map never connects to its configured provider');
        $node->safe_psql('postgres', 'GRANT EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) TO map_reader;');
        $reader->query_safe("SET semloom_pg.gateway_socket='invalid-relative-path'");
        is($capture->('EXECUTE map_acl(1)'), '22023|SemLoom provider socket path must be absolute',
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
    $hooks->query_safe("SET semloom_pg.gateway_socket='invalid-relative-path'");
    is($hooks->query_safe(q{SELECT map_test_capture('EXECUTE hook_child')}),
       '22023|SemLoom provider socket path must be absolute', 'authorized execution reaches the provider after child initialization');
    is($hooks->query_safe('SELECT map_events()'), '1|1|1', 'authorized Map and ordinary child initialize once');
    $node->safe_psql('postgres', 'REVOKE EXECUTE ON FUNCTION ai_semantic.map(text,text,jsonb) FROM map_reader;');
    $hooks->query_safe("SET semloom_pg.gateway_socket='$socket_path'; SET ROLE map_reader; SELECT map_watch($marker_oid, $child_oid)");
    is($hooks->query_safe(q{SELECT map_test_capture('EXPLAIN EXECUTE hook_child')}),
       '42501|permission denied for function map', 'permission is checked before even the explain-only child initializer');
    my @denied_events = split /\|/, $hooks->query_safe('SELECT map_events()');
    is($denied_events[0], 0, 'failed ACL does not invoke the Map execution hook');
    is($denied_events[1], 0, 'failed ACL does not initialize the ordinary child function');
    $hooks->quit;

    my $qualified = 'ai_semantic.map(text,text,jsonb)';
    my $identity = $node->background_psql('postgres');
    $identity->query_safe("SET plan_cache_mode=force_generic_plan; PREPARE map_identity(integer) AS $query WHERE id=\$1;");
    like($identity->query_safe('EXPLAIN EXECUTE map_identity(1)'), qr/Custom Scan \(SemLoom SemMap\)/,
         'new Map generic plan initially uses its extension member');
    my $ordinary_definition = q{
CREATE OR REPLACE FUNCTION ai_semantic.map(input text, instruction text, options jsonb) RETURNS text
LANGUAGE plpgsql VOLATILE AS $$ BEGIN RETURN 'ordinary:' || input; END $$;
};
    $node->safe_psql('postgres', "ALTER EXTENSION semloom_pg DROP FUNCTION $qualified; $ordinary_definition");
    is($node->safe_psql('postgres', "SELECT '$qualified'::regprocedure::oid"), $marker_oid,
       'detached function replacement keeps its OID');
    is($identity->query_safe('EXECUTE map_identity(1)'), '1|ordinary:hello',
       'same-OID body replacement invalidates the cached new Map plan');
    unlike($identity->query_safe('EXPLAIN EXECUTE map_identity(1)'), qr/Custom Scan/,
           'revalidated generic plan executes the ordinary non-member function');
    $node->safe_psql('postgres', "ALTER EXTENSION plpgsql ADD FUNCTION $qualified");
    unlike($node->safe_psql('postgres', "EXPLAIN $query"), qr/Custom Scan/,
           'membership in another extension does not qualify the new signature');
    $node->safe_psql('postgres', "ALTER EXTENSION plpgsql DROP FUNCTION $qualified; DROP FUNCTION $qualified; $ordinary_definition");
    isnt($node->safe_psql('postgres', "SELECT '$qualified'::regprocedure::oid"), $marker_oid,
          'drop and recreate changes the new signature OID');
    is($identity->query_safe('EXECUTE map_identity(1)'), '1|ordinary:hello',
       'cached statement resolves a newly created ordinary function');
    $node->safe_psql('postgres', "DROP FUNCTION $qualified; $map_definition; ALTER EXTENSION semloom_pg ADD FUNCTION $qualified;");
    like($identity->query_safe('EXPLAIN EXECUTE map_identity(1)'), qr/Custom Scan \(SemLoom SemMap\)/,
         'restored extension member is lowered after generic-plan invalidation');

    my $member_definition = $node->safe_psql('postgres', $definition_query);
    $node->safe_psql('postgres', "ALTER EXTENSION semloom_pg DROP FUNCTION $qualified");
    my $before_drop = $identity->query_safe(q{SELECT map_test_capture('EXPLAIN EXECUTE map_identity(1)')});
    note("member-only DROP before refresh: $before_drop");
    $identity->query_safe('DISCARD PLANS');
    unlike($identity->query_safe('EXPLAIN EXECUTE map_identity(1)'), qr/Custom Scan/,
           'manual refresh in the caching backend removes member-only Map lowering');
    $node->safe_psql('postgres', "ALTER EXTENSION semloom_pg ADD FUNCTION $qualified");
    my $before_add = $identity->query_safe('EXPLAIN EXECUTE map_identity(1)');
    note("member-only ADD before refresh: $before_add");
    $identity->query_safe('DISCARD PLANS');
    like($identity->query_safe('EXPLAIN EXECUTE map_identity(1)'), qr/Custom Scan \(SemLoom SemMap\)/,
         'manual refresh in the caching backend restores member-only Map lowering');
    is($node->safe_psql('postgres', $definition_query), $member_definition,
       'member-only ADD and DROP did not change the function body');
    $identity->quit;

    $node->safe_psql('postgres', 'CREATE TABLE map_target(id integer, generated text)');
    like($node->safe_psql('postgres', "EXPLAIN INSERT INTO map_target $query"),
         qr/Custom Scan \(SemLoom SemMap\)/, 'direct INSERT SELECT uses the same new Map plan');
    is($node->safe_psql('postgres', "SET semloom_pg.gateway_socket='invalid-relative-path'; SELECT map_test_capture(\$sql\$INSERT INTO map_target $query\$sql\$)"),
       '22023|SemLoom provider socket path must be absolute', 'INSERT cannot silently invoke an old executor');
    is($node->safe_psql('postgres', 'SELECT count(*) FROM map_target'), 0, 'failed provider INSERT leaves no target rows');
    for my $mode ('force_custom_plan', 'force_generic_plan')
    {
        for my $source (
            ['instruction', 'text', '$1', $options, q|'Echo the input.'|],
            ['options', 'jsonb', q|'Echo the input.'|, '$1', $options])
        {
            my ($label, $type, $instruction, $config, $value) = @$source;
            my ($result, $output, $error) = $node->psql('postgres',
                "\\set VERBOSITY verbose\nSET plan_cache_mode=$mode; PREPARE map_insert($type) AS " .
                "INSERT INTO map_target SELECT id, ai_semantic.map(body, $instruction, $config) FROM ONLY map_inputs; " .
                "EXPLAIN EXECUTE map_insert($value);");
            isnt($result, 0, "$mode rejects INSERT source $label parameter");
            like($error, qr/ERROR:  0A000: SemMap instruction and options must be fixed immutable constants\n/,
                 'INSERT source receives the same pre-substitution constant validation');
        }
    }

    for my $unsupported (
        ['constant CASE', "SELECT CASE WHEN true THEN ai_semantic.map(body, 'Echo the input.', $options) ELSE '' END FROM ONLY map_inputs",
         'ai_semantic.map is only supported as a top-level output expression'],
        ['WHERE expression', "SELECT id FROM ONLY map_inputs WHERE ai_semantic.map(body, 'Echo the input.', $options) = 'hello'",
         'ai_semantic.map is only supported as a top-level output expression'],
        ['Map and Filter', "$query WHERE ai_semantic.filter(body)",
         'SemMap and SemFilter cannot be combined in the current capability'],
        ['input subquery', "SELECT ai_semantic.map((SELECT 'hello'::text), 'Echo the input.', $options) FROM ONLY map_inputs",
         'query shape is outside the current SemMap capability'],
        ['predicate subquery', "$query WHERE id = (SELECT 1)", 'query shape is outside the current SemMap capability'],
        ['ORDER BY', "$query ORDER BY id", 'query shape is outside the current SemMap capability'],
        ['DISTINCT', "SELECT DISTINCT ai_semantic.map(body, 'Echo the input.', $options) FROM ONLY map_inputs",
         'query shape is outside the current SemMap capability'],
        ['JOIN', "$query JOIN ONLY map_target USING(id)", 'the SemMap capability requires exactly one base relation'],
        ['RETURNING', "INSERT INTO map_target $query RETURNING id", 'INSERT shape is outside the current SemMap capability'],
        ['ON CONFLICT', "INSERT INTO map_target $query ON CONFLICT DO NOTHING", 'INSERT shape is outside the current SemMap capability'],
        ['OVERRIDING', "INSERT INTO map_target OVERRIDING SYSTEM VALUE $query", 'INSERT shape is outside the current SemMap capability'])
    {
        my ($label, $statement, $message) = @$unsupported;
        my ($result, $output, $error) = $node->psql('postgres', "\\set VERBOSITY verbose\nEXPLAIN $statement;");
        isnt($result, 0, "$label is not silently enabled by adding Map");
        like($error, qr/ERROR:  0A000: \Q$message\E\n/, "$label has an explicit unsupported-shape error");
    }
}
ok(!IO::Select->new($listener)->can_read(0), 'plan and invalid-path checks made zero provider connections');
close($listener);
unlink($socket_path) or die "could not remove provider connection sentinel";
$node->stop;
done_testing();
