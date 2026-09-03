use strict;
use warnings FATAL => 'all';

use JSON::PP qw(decode_json);
use IO::Select;
use IO::Socket::UNIX;
use Socket qw(SOCK_STREAM);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('map_plan');
$node->init;
my $socket_path = $node->host . '/map-never-connect.sock';
my $listener = IO::Socket::UNIX->new(Type => SOCK_STREAM, Local => $socket_path, Listen => 8)
  or die "could not create provider connection sentinel";
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
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
}
ok(!IO::Select->new($listener)->can_read(0), 'plan-only Map made zero provider connections');
close($listener);
unlink($socket_path) or die "could not remove provider connection sentinel";
$node->stop;
done_testing();
