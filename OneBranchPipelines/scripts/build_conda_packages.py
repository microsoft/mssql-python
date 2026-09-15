#!/usr/bin/env python3
"""Legacy build entrypoint; implementation lives in eng.conda_tools."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from eng.conda_tools.__main__ import build_main

if __name__ == "__main__":
    sys.exit(build_main())
