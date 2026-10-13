"""Finite real-library query sharing check, with local fixture responses only."""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from src.experiments.query_sharing_lotus import main

if __name__=='__main__':
    raise SystemExit(main())
