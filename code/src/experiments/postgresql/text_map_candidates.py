"""Declared native supply candidates; this module starts no service or query."""
from dataclasses import asdict

from .query_config import QueryConfig

CAPACITIES = (16, 64, 128)


def native_candidate(role, capacity, table, rows, *, ray_address=None):
    if capacity not in CAPACITIES or role not in ('ray-data', 'daft-native'):
        raise ValueError('use a declared native role and finite candidate capacity')
    options = dict(concurrency=capacity,max_posts=rows,query_timeout_s=120)
    if role == 'ray-data':
        if ray_address is None:
            raise ValueError('native Ray candidate requires the shared runtime address')
        actors = {16:1, 64:2, 128:4}[capacity]
        options.update(ray_actors=actors,ray_async_batches_per_actor=capacity//actors,
            ray_batch_rows=1,ray_read_blocks=4,ray_read_concurrency=2,
            ray_num_cpus=8,ray_object_store_bytes=268435456,ray_address=ray_address)
    else:
        options.update(daft_num_threads=8,daft_read_partitions=4)
    key = role+'-c'+str(capacity)
    return dict(id=key,role=role,config=asdict(QueryConfig(key,role,'map',table,**options)))


def main_candidates(table, rows, *, ray_address, transport_path, transport_sha256):
    """Equal finite opportunities for the main system and three distinct controls."""
    candidates=[]
    for role in ('pg-daft-ray','pg-source-direct','ray-data','daft-native'):
        for capacity in CAPACITIES:
            if role in ('ray-data','daft-native'):
                candidates.append(native_candidate(role,capacity,table,rows,ray_address=ray_address))
                continue
            key=role+'-c'+str(capacity)
            options=dict(concurrency=capacity,window=256,max_posts=rows,query_timeout_s=120,
                input_bytes=128*1048576,result_bytes=128*1048576,
                pg_window_bytes=256*1048576,pg_staging_bytes=4*1048576,
                ray_num_cpus=8,ray_object_store_bytes=268435456)
            if role=='pg-daft-ray':
                options.update(pg_total_budget=True,map_transport_config=transport_path,
                               map_transport_sha256=transport_sha256)
            config=QueryConfig(key,'pg' if role=='pg-daft-ray' else role,'map',table,**options)
            candidates.append(dict(id=key,role=role,config=asdict(config)))
    return candidates


if __name__ == '__main__':
    import argparse
    import json
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table',required=True)
    parser.add_argument('--rows',required=True,type=int)
    parser.add_argument('--ray-address',required=True)
    parser.add_argument('--transport-path',required=True)
    parser.add_argument('--transport-sha256',required=True)
    args=parser.parse_args()
    values=main_candidates(args.table,args.rows,ray_address=args.ray_address,
        transport_path=args.transport_path,transport_sha256=args.transport_sha256)
    print(json.dumps(dict(profile='main',candidates=values,model_requests=0,
        executable_stage=False,requires='identified inputs, services, orders, finite ledger and authorization'),indent=2))
