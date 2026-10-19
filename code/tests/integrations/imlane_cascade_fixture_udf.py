"""Same author UDF entry with an explicit CPU engine child, never a model."""

import functools

from integrations.imlane_records import cascade_udf
from tests.integrations.imlane_cascade_fixture import CpuCascadeProcess


cascade_udf.CascadeWorker = functools.partial(cascade_udf.CascadeWorker, engine_type=CpuCascadeProcess)
infer_records = cascade_udf.infer_records
