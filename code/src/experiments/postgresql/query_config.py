"""Declared query arm, task and resource configuration for one immutable source."""
from dataclasses import dataclass


@dataclass(frozen=True)
class QueryConfig:
    unit_id: str
    arm: str
    task: str
    table: str
    concurrency: int = 2
    window: int = 4
    input_bytes: int = 134217728
    result_bytes: int = 67108864
    pg_window_bytes: int = 67108864
    pg_total_budget: bool = False
    pg_staging_bytes: int = 16777216
    organization_config: str | None = None
    organization_sha256: str | None = None
    query_timeout_s: float = 300
    max_posts: int | None = None
    movie_id: str | None = None
    result_order: str = 'input'
    ray_read_blocks: int = 2
    ray_read_concurrency: int = 2
    ray_batch_rows: int = 32

    def __post_init__(self):
        import math
        import re
        if self.arm not in ('pg','pg-source-direct','ray-data','lotus') or self.task not in ('map','movie-q1','movie-q2','movie-q3'):
            raise ValueError('unknown query arm or task')
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}',self.unit_id):
            raise ValueError('invalid query unit identity')
        if self.arm in ('pg-source-direct','ray-data') and self.task!='map':
            raise ValueError('this native/direct entry supports only Map tasks')
        if self.arm=='lotus' and self.task=='map':
            raise ValueError('LOTUS entry uses original Movie Q1/Q2/Q3 programs')
        if self.task!='map' and self.movie_id is not None:
            raise ValueError('original Movie tasks own their movie predicate')
        if self.max_posts is not None and (type(self.max_posts) is not int or self.max_posts<1):
            raise ValueError('positive maximum POST count required')
        for name in ('concurrency','window','input_bytes','result_bytes','pg_window_bytes',
                     'pg_staging_bytes','ray_read_blocks','ray_read_concurrency','ray_batch_rows'):
            if type(getattr(self,name)) is not int or getattr(self,name)<1:
                raise ValueError('positive query resource limits required')
        if max(self.concurrency,self.window,self.ray_read_blocks,self.ray_read_concurrency)>256:
            raise ValueError('query concurrency/window exceeds the supported range')
        if type(self.pg_total_budget) is not bool:
            raise ValueError('PG total budget flag must be boolean')
        if self.pg_total_budget and (self.arm!='pg' or self.task!='map'):
            raise ValueError('total retained-window budget currently applies to PG Map')
        if (self.organization_config is None) != (self.organization_sha256 is None):
            raise ValueError('organization configuration requires its declared SHA-256')
        if self.organization_config is not None:
            if (self.arm != 'pg' or self.task != 'map' or not self.pg_total_budget
                    or not isinstance(self.organization_config, str) or not self.organization_config
                    or not isinstance(self.organization_sha256, str)
                    or not re.fullmatch(r'[0-9a-f]{64}', self.organization_sha256)):
                raise ValueError('organization controls require total-budget PG Map and a configuration identity')
        if max(self.input_bytes,self.result_bytes,self.pg_window_bytes,self.pg_staging_bytes)>256*1048576:
            raise ValueError('query byte configuration exceeds the supported range')
        if self.pg_staging_bytes<65536:
            raise ValueError('PG staging bytes are below the supported minimum')
        if (type(self.query_timeout_s) not in (int,float) or not math.isfinite(self.query_timeout_s)
                or not 0 < self.query_timeout_s <= 3600):
            raise ValueError('query deadline must be positive and bounded')
        if self.result_order not in ('input','completion'):
            raise ValueError('unsupported direct result order')
