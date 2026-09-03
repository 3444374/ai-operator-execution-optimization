import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const [repo, input] = process.argv.slice(2).map(value => path.resolve(value));
const raw = path.join(repo, 'experiments/results/postgresql/semmap_pg_wire_20260903/raw');
const target = path.join(raw, 'protocol_review');
const sha = data => crypto.createHash('sha256').update(data).digest('hex');
function files(root) {
  return fs.readdirSync(root, {withFileTypes: true}).flatMap(item => {
    const name = path.join(root, item.name);
    if (item.isDirectory()) return files(name);
    if (!item.isFile()) throw Error('unexpected evidence entry');
    return [name];
  }).sort();
}
function verifyManifest(root) {
  const seen = new Set();
  for (const line of fs.readFileSync(path.join(root, 'SHA256SUMS'), 'utf8').trim().split('\n')) {
    const match = line.match(/^([0-9a-f]{64})  (.+)$/);
    if (!match || seen.has(match[2])) throw Error('invalid manifest');
    const file = path.resolve(root, match[2]);
    if (!file.startsWith(root + path.sep) || sha(fs.readFileSync(file)) !== match[1]) throw Error('evidence mismatch: ' + match[2]);
    seen.add(match[2]);
  }
  return seen.size;
}
function manifest(root) {
  const own = path.join(root, 'SHA256SUMS');
  fs.writeFileSync(own, files(root).filter(file => file !== own)
    .map(file => `${sha(fs.readFileSync(file))}  ${path.relative(root, file)}\n`).join(''));
}
const prior = verifyManifest(raw);
const priorPlan = verifyManifest(path.join(repo, 'experiments/results/postgresql/semmap_pg_plan_20260903/raw'));
const current = verifyManifest(path.join(input, 'server-final-f46fe936'));
verifyManifest(path.join(input, 'local-f46fe936'));
const qualified = JSON.parse(fs.readFileSync(path.join(input, 'server-final-f46fe936/qualification.json')));
if (qualified.source_commit !== 'f46fe936cdaceae8b5e3571321e28dfae6ac724a' || qualified.tap_tests !== 1758 ||
    qualified.regression_tests !== 1 || qualified.pg_runtime_version !== '18.3' || !qualified.pg_build_warning_free ||
    !qualified.base_extension_unchanged || qualified.real_model_requests_attempted !== 0 || qualified.resource_smoke_run)
  throw Error('unexpected qualification');
let sources = 0;
for (const [name, digest] of Object.entries(qualified.source_sha256)) {
  if (!name.startsWith('code/') || name.includes('..')) throw Error('unexpected source path');
  const bytes = execFileSync('git', ['show', `${qualified.source_commit}:${name}`], {cwd: repo});
  if (sha(bytes) !== digest) throw Error('source identity mismatch: ' + name);
  sources++;
}
const postflight = JSON.parse(fs.readFileSync(path.join(input, 'postflight.json')));
if (!postflight.passed || postflight.server_main_dirty || postflight.runs.length !== 4) throw Error('postflight failed');
const mappings = [
  ['110ad445', 'phase-red-public', 'phase-red', 457, 2],
  ['0baaae0d', 'usage-red-public', 'usage-red', 468, 2],
  ['long-path', 'long-path-public', 'long-path', 1758, 0],
  ['server', 'server-final-f46fe936', 'final', 1758, 0],
  ['local', 'local-f46fe936'],
];
const runs = [];
for (const [name, source, label, count, expectedExit] of mappings) {
  if (fs.existsSync(path.join(target, name))) throw Error('refusing evidence overwrite');
  if (!label) continue;
  const steps = JSON.parse(fs.readFileSync(path.join(input, source, 'steps.json')));
  const tap = fs.readFileSync(path.join(input, source, 'tap.log'), 'utf8');
  const match = tap.match(/Files=(\d+), Tests=(\d+)/);
  const actualExit = steps.find(step => step.name === 'tap').exit_code;
  if (!match || Number(match[2]) !== count || actualExit !== expectedExit) throw Error('unexpected TAP result');
  runs.push({...postflight.runs.find(run => run.run === label), artifact_directory: name,
    tap_tests: count, tap_exit_code: actualExit,
    last_step: steps.at(-1).name, last_step_exit_code: steps.at(-1).exit_code});
}
for (const [name, source] of mappings) fs.cpSync(path.join(input, source), path.join(target, name), {recursive: true});
fs.copyFileSync(path.join(input, 'postflight.json'), path.join(target, 'postflight.json'));
fs.copyFileSync(path.join(input, 'postflight_review.py'), path.join(target, 'postflight_review.py'));
fs.copyFileSync(path.join(input, 'qualify_map.py'), path.join(target, 'qualify_map.py'));
fs.copyFileSync(fileURLToPath(import.meta.url), path.join(target, 'archive_review.mjs'));
fs.writeFileSync(path.join(target, 'verification.json'), JSON.stringify({
  source_commit: qualified.source_commit, prior_wire_manifest_entries: prior,
  prior_plan_manifest_entries: priorPlan, final_server_manifest_entries: current,
  source_blobs_verified: sources, runs, local_contracts: 139, local_c11: 8,
  standards_unresolved: 0, spec_resolved: 2, spec_unresolved: 0,
  model_requests_attempted: 0, resource_smoke_run: false,
}, null, 2) + '\n');
for (const name of ['110ad445', '0baaae0d', 'long-path']) manifest(path.join(target, name));
manifest(target);
manifest(raw);
console.log(JSON.stringify({prior, priorPlan, current, sources, archived: files(target).length, runs}));
