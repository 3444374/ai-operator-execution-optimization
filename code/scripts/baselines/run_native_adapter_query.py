#!/usr/bin/env python3
"""Run one finite external-input method/executor comparison."""
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from src.experiments.postgresql.native_adapter_query import main

if __name__=='__main__':
    raise SystemExit(main())
