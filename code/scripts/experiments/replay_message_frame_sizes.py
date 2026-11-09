#!/usr/bin/env python3
"""CPU replay for exact prepared-message frame-size reuse; starts no model."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.research.data_organization.message_frame_replay import main


if __name__ == '__main__':
    raise SystemExit(main())
