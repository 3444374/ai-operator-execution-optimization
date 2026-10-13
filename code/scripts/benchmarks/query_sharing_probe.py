"""Run the finite, model-free query sharing probe from the repository root."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.experiments.query_sharing_probe import main

if __name__ == '__main__':
    main()
