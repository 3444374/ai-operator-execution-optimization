"""Audit per-operator PG memory traces; sums of peaks are not measured query RSS."""
import json
import re

CONVERSION_LIMIT=1048576


def verify_window_memory(lines, *, backend_pid, retained_limit, staging_limit, window):
    nodes=[]
    seen=set()
    for line in lines:
        match=re.search(r'LOG:\s+SEMLOOM_WINDOW_MEMORY (\{.*\})\s*$',line)
        if match is None:continue
        value=json.loads(match.group(1))
        if value.get('backend_pid')!=backend_pid:continue
        integers=('memory_id','retained_rows','pending_rows','retained_bytes','retained_limit','staging_limit',
                  'peak_retained_bytes','peak_staging_bytes','peak_conversion_bytes','peak_receive_bytes',
                  'peak_retained_rows','memory_waits')
        if value.get('version')!=1 or any(type(value.get(k)) is not int or value[k]<0 for k in integers):
            raise ValueError('invalid PG window memory observation')
        if value['memory_id'] in seen:
            raise ValueError('duplicate operator memory observation')
        seen.add(value['memory_id'])
        if value['retained_limit']!=retained_limit or value['staging_limit']!=staging_limit:
            raise ValueError('PG operator memory configuration differs')
        if (value['peak_retained_bytes']>retained_limit or value['peak_staging_bytes']>staging_limit or
                value['peak_conversion_bytes']>CONVERSION_LIMIT or value['peak_retained_rows']>window):
            raise ValueError('PG operator exceeded a memory allocation bound')
        if value['retained_rows'] or value['pending_rows'] or value['retained_bytes']:
            raise ValueError('PG operator retained a row after cleanup')
        nodes.append(value)
    if not nodes:raise ValueError('missing PG operator memory cleanup evidence')
    return dict(operators=len(nodes),nodes=nodes,
                sum_retained_limits=sum(v['retained_limit'] for v in nodes),
                sum_staging_limits=sum(v['staging_limit'] for v in nodes),
                sum_conversion_limits=CONVERSION_LIMIT*len(nodes),
                sum_retained_peaks=sum(v['peak_retained_bytes'] for v in nodes),
                interpretation='per-operator accounted row contexts and metadata reservation; sums are not concurrent query RSS')
