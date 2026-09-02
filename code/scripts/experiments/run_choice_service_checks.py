#!/usr/bin/env python3
"""Run the bounded old/choice SQL-to-HTTP check against an identified service."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.choice_service_checks import main

if __name__ == '__main__':
    main()
