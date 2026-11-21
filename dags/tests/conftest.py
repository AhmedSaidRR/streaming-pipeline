"""Make the DAG folder importable when these tests run in the producer test container."""

import sys
from pathlib import Path

DAG_DIR = str(Path(__file__).resolve().parents[1])
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
