"""Account observed transport reservations, terminals and backing-buffer lifetimes."""

def ray_transport_accounting(events, requests):
    """Check row terminals and object lifetimes without inventing remote occupancy."""
    completed, live, seen, placed = [], {}, set(), set()
    peak = 0
    for event in events:
        if event['event'] == 'core_ray_http_completed':
            completed.append((event['key']['session_id'], event['key']['sequence']))
        if event['event'] == 'core_ray_block_reserved':
            identity = event['block_id']
            if identity in seen or event['bytes'] <= 0 or event['rows'] <= 0:
                raise ValueError('Ray block identity or size differs')
            seen.add(identity)
            live[identity] = event['bytes']
        elif event['event'] == 'core_ray_block_put':
            identity = event['block_id']
            if identity not in live or identity in placed or event['bytes'] != live[identity]:
                raise ValueError('Ray put has no matching reservation')
            placed.add(identity)
        elif event['event'] == 'core_ray_block_released':
            if event['block_id'] not in live:
                raise ValueError('Ray object release has no matching put')
            del live[event['block_id']]
        else:
            continue
        actual = sum(live.values())
        if actual != event['object_bytes'] or actual > event['object_limit_bytes']:
            raise ValueError('Ray object ownership exceeds or differs from its ledger')
        peak = max(peak, actual)
    if live or seen != placed or len(completed) != requests or len(set(completed)) != requests:
        raise ValueError('Ray requests or objects did not settle exactly once')
    return dict(completed_requests=len(completed), object_blocks=len(seen),
                peak_arrow_buffer_bytes=peak, final_arrow_buffer_bytes=0,
                scope='owned Arrow backing buffers; not total process or Ray object-store memory')
