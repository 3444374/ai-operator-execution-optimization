use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use IPC::Run;
use JSON::PP qw(encode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $node = PostgreSQL::Test::Cluster->new('map_execution');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'semloom_pg'\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION semloom_pg;
CREATE TABLE map_rows(id integer, body text);
INSERT INTO map_rows VALUES (1, 'hello');
});
my $socket = $node->host . '/map.sock';
my $fixture = $node->basedir . '/map.json';
open(my $file, '>', $fixture) or die 'cannot create Map fixture';
print $file encode_json({
    'e97d97db3b315860ef5a0b39258908945f74651b94b68f4d3c319800d680266d' => {
        raw_output => 'hello', response_model_id => 'golden-map-v1',
        prompt_tokens => 17, output_tokens => 1, finish_reason => 'stop',
    },
});
close($file);
my $gateway_script = abs_path("$FindBin::RealBin/../gateway/recording_gateway.py");
my ($out, $err) = ('', '');
my $gateway = IPC::Run::start(['python3', $gateway_script, '--socket', $socket,
    '--once', '--golden-fixture', $fixture], '>', \$out, '2>', \$err);
for (1 .. 200) { last if -S $socket; sleep(0.01); }
ok(-S $socket, 'Map golden gateway listens') or diag($err);
my $options = q|' {"model":"golden-map-v1","temperature":0,"max_tokens":128}'::jsonb|;
my ($status, $stdout, $stderr) = $node->psql('postgres', qq{
SET statement_timeout='5s';
SET semloom_pg.gateway_socket='$socket';
SELECT id, ai_semantic.map(body, 'Echo the input.', $options) FROM ONLY map_rows;
});
is($status, 0, 'generative Map executes through PostgreSQL and golden wire v5') or diag($stderr);
is($stdout, '1|hello', 'the independent ASCII vector returns generated text');
if ($status == 0) { $gateway->finish; }
else { $gateway->kill_kill; }
$node->stop;
done_testing();
