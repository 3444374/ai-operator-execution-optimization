"""Deterministic Movie-derived Map inputs, excluding earlier experimental data."""
import csv
import hashlib
import heapq
from pathlib import Path

from .query_workloads import file_identity, prepare


def prepare_movie_partitions(source, previous, root, *, tuning_rows=512, evaluation_rows=1024, seed='m1-real-20260914'):
    """Group by movie, exclude previous movies/IDs/text, and select by hash rank.

This is a new Map workload from original reviews, not a replay of the upstream
SemBench query program. Labels validate eligibility; they never set the rank.
"""
    if any(type(n) is not int or not 1 <= n <= 6000 for n in (tuning_rows,evaluation_rows)):
        raise ValueError('partition sizes must be between one and 6000')
    source, previous, root = Path(source), Path(previous), Path(root)
    source_identity, previous_identity = file_identity(source), file_identity(previous)
    old_movies, old_ids, old_texts = set(), set(), set()
    with previous.open(newline='') as stream:
        for row in csv.DictReader(stream):
            old_movies.add(row['id']);old_ids.add(row['reviewId'])
            old_texts.add(hashlib.sha256(row['reviewText'].encode()).hexdigest())
    limits = {'tuning':tuning_rows,'evaluation':evaluation_rows}
    heaps = {key:[] for key in limits}
    seen_ids, seen_texts = set(), set()
    counts = dict(source_rows=0,invalid=0,prior_excluded=0,duplicate=0,eligible=0)
    with source.open(newline='') as stream:
        reader = csv.DictReader(stream)
        if not {'id','reviewId','reviewText','scoreSentiment'} <= set(reader.fieldnames or ()):
            raise ValueError('source lacks original Movie fields')
        for position,row in enumerate(reader):
            counts['source_rows'] += 1
            if counts['source_rows'] > 2000000:
                raise ValueError('offline population exceeds declared two-million-row bound')
            movie,identity,text,label = (row[k] for k in ('id','reviewId','reviewText','scoreSentiment'))
            if not movie or not identity or not text.strip() or len(text.encode())>65536 or label not in ('POSITIVE','NEGATIVE'):
                counts['invalid'] += 1;continue
            digest=hashlib.sha256(text.encode()).hexdigest()
            if movie in old_movies or identity in old_ids or digest in old_texts:
                counts['prior_excluded'] += 1;continue
            if identity in seen_ids or digest in seen_texts:
                counts['duplicate'] += 1;continue
            seen_ids.add(identity);seen_texts.add(digest);counts['eligible'] += 1
            group = 'tuning' if hashlib.sha256((seed+':movie:'+movie).encode()).digest()[0]%2==0 else 'evaluation'
            rank = int.from_bytes(hashlib.sha256((seed+':review:'+identity+':'+digest).encode()).digest(),'big')
            example = ('movie-original-'+str(position),movie,text,label,identity)
            item=(-rank,position,example,digest)
            heap=heaps[group]
            if len(heap)<limits[group]:heapq.heappush(heap,item)
            elif item>heap[0]:heapq.heapreplace(heap,item)
    if any(len(heaps[key])!=limits[key] for key in limits):
        raise ValueError('insufficient unused Movie groups')
    if file_identity(source)!=source_identity or file_identity(previous)!=previous_identity:
        raise ValueError('source changed during offline selection')
    selected = {key:sorted(heap,key=lambda item:(-item[0],item[1])) for key,heap in heaps.items()}
    assert not ({item[2][1] for item in selected['tuning']} & {item[2][1] for item in selected['evaluation']})
    report=dict(schema='semloom.movie_partition.v1',seed=seed,source=source_identity,previous=previous_identity,
        counts=counts,excluded_prior_movies=len(old_movies),excluded_prior_ids=len(old_ids),
        selection='movie-hash partition, lowest review hash ranks; first unique review ID and exact text occurrence',
        role='new Movie-derived Map workload; not original SemBench program',splits={})
    for key,items in selected.items():
        manifest=prepare(root/key,'movie',[item[2] for item in items],
            dict(source_sha256=source_identity['sha256'],previous_sha256=previous_identity['sha256'],
                 seed=seed,partition=key,selection=report['selection']),max_rows=limits[key])
        report['splits'][key]=dict(rows=len(items),manifest_sha256=manifest['sha256'],
            movies=len({item[2][1] for item in items}),positive=sum(item[2][3]=='POSITIVE' for item in items))
    return report
