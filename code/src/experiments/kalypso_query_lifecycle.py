"""Source/sink and observation adapter; the author service owns all Filter work."""
import json
from pathlib import Path

from src.baselines.text.frameworks.kalypso_native import adapter as native
from src.baselines.text.frameworks.kalypso_native.native_server import server_argv


def run_attached(cfg,unit,output,observer=None):
    server_argv(cfg)
    native.load_dependencies(cfg)
    from src.experiments import native_query_lifecycle as sessions
    from src.observability.native_query_timing import ObservationStore,timed_driver_type,query_timing
    # Register this external lifecycle identity; the original sequence logic is unchanged.
    previous=sessions.BACKENDS,sessions.RESIDENT
    sessions.BACKENDS=previous[0]|{'kalypso-native'}
    sessions.RESIDENT=previous[1]|{'kalypso-native'}
    store=ObservationStore();driver=timed_driver_type(native.NativeQueryDriver,store)
    try:
        result=sessions.run_sequence(unit['queries'],lambda path,first:driver(cfg,unit,path),
            output,backend='kalypso-native',lifecycle='resident',observer=observer)
    finally:
        sessions.BACKENDS,sessions.RESIDENT=previous
    for q in result['queries']:
        q['complete_query_timing']=query_timing(q)
    result['service_lifecycle_scope']='external GPU owner: startup/warmup/queries/shutdown all charged to unit total'
    return result


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--unit',required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    cfg=json.loads(args.config.read_text())
    server_argv(cfg)  # Reject legacy engine overrides before client/service attachment.
    unit=next(u for u in cfg['units'] if u['unit_id']==args.unit)
    cfg.update(url='http://127.0.0.1:'+str(cfg['port']),native_trace=str(args.output.parent/'native-trace.jsonl'),
               loaded_model_proof=str(args.output.parent/'loaded-model.json'),
               sembench=str(Path(cfg['baseline_packet'])/'sembench'))
    if not cfg['execution_enabled'] or cfg.get('CPU_fixture_only'):
        raise ValueError('GPU owner must explicitly bind an enabled private runtime config')
    if args.output.exists():raise ValueError('Output exists; no automatic request replay')
    save=native.load_dependencies(cfg)
    from src.observability.records_telemetry import TelemetryEvents
    with TelemetryEvents(args.output.parent/'client-events.jsonl') as events:
        result=run_attached(cfg,unit,args.output,observer=events)
    save(args.output/'complete-query-report.json',result)
    print(json.dumps(dict(status=result['status'],unit=args.unit,queries=len(result['queries']))))


if __name__=='__main__':main()
