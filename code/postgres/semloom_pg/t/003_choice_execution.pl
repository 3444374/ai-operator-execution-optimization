use strict;
use warnings FATAL => 'all';
use utf8;

use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(encode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $node = PostgreSQL::Test::Cluster->new('choice_execution');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE choice_rows(id integer, content text);
INSERT INTO choice_rows VALUES (1, U&'\6570\636E\5E93\+01F642'), (2, NULL);
});
my $socket = $node->host . '/choice.sock';
my $fixture = $node->basedir . '/choice.json';
open(my $file, '>', $fixture) or die "cannot create golden fixture";
print $file encode_json({
    '0d587219759ce92992da90a8af1fc40baefff79ab79861d9930886c667dc7fa1' => 'TRUE',
});
close($file);
my $gateway_script = abs_path("$FindBin::RealBin/../gateway/recording_gateway.py");
my ($gateway_out, $gateway_err) = ('', '');
my $gateway = IPC::Run::start(['python3', $gateway_script, '--socket', $socket,
    '--once', '--golden-fixture', $fixture], '>', \$gateway_out, '2>', \$gateway_err);
for (1 .. 200) { last if -S $socket; sleep(0.01); }
ok(-S $socket, 'choice golden gateway listens') or diag($gateway_err);
my $options = q|'{"model":"golden-model-v1","temperature":0,"max_tokens":8,"generation_profile":"semloom.generation.choice.tristate.v1"}'::jsonb|;
my ($status, $stdout, $stderr) = $node->psql('postgres', qq{
SET semloom_pg.gateway_socket = '$socket';
SELECT id FROM choice_rows WHERE ai_semantic.filter(content, 'Classify input.', $options);
});
is($status, 0, 'choice executes through PostgreSQL and golden wire v4') or diag($stderr);
is($stdout, '1', 'choice preserves the TRUE row and drops SQL NULL');
if ($status == 0) { $gateway->finish; }
else { $gateway->kill_kill; }
$node->stop;
done_testing();
