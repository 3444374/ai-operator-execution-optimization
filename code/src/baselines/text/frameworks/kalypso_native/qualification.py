"""Offline audit comparison; never changes the author sampling options."""

def sampling_matches(actual,expected):
    if set(actual)!=set(expected):return False
    a=dict(actual);e=dict(expected)
    if a['stop'] is None:a['stop']=[]
    if e['stop'] is None:e['stop']=[]
    return a==e
