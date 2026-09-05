import contextlib, hashlib, json, os, platform, subprocess, sys, unittest
from pathlib import Path
source, output = map(Path, sys.argv[1:3])
source, output = source.resolve(), output.resolve()
os.chdir(source)
sys.path[:0] = [str(source/'code'), str(source/'code/tests/experiments'), str(source/'code/tests/observability')]
groups = {
 'resource': ['test_resource_lifecycle','test_resource_measurement_contracts','test_resource_qualification','test_semmap_resource_cli','test_semmap_resource_gateway_observer','test_semmap_resource_runner'],
 'attribution': ['test_provider_session_attribution'],
 'process': ['test_proc_collection_validity','test_process_recorder','test_process_resources','test_process_resources_linux'],
 'sampling_lifecycle': ['test_sampling_lifecycle'],
 'v1_replay': ['test_v1_resource_gate_characterization'],
}
if sys.platform.startswith('linux'):
 groups['choice']=['test_choice_attempt_ledger','test_choice_http_observer','test_choice_service_checks']
report={'source_commit':subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip(),'platform':platform.system(),'python':platform.python_version(),'groups':{}}
output.mkdir(exist_ok=True)
for group, modules in groups.items():
 p=output/(group+'-final.txt')
 with p.open('w') as handle, contextlib.redirect_stdout(handle), contextlib.redirect_stderr(handle):
  result=unittest.TextTestRunner(stream=handle,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromNames(modules))
 report['groups'][group]={'run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skips':len(result.skipped),'skip_reasons':[r for _,r in result.skipped],'modules':modules,'log_sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
report['total']=sum(x['run'] for x in report['groups'].values())
(output/'tests-final.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
sys.exit(int(any(x['failures'] or x['errors'] for x in report['groups'].values())))
