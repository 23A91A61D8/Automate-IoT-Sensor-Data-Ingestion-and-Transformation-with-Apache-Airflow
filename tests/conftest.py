"""
conftest.py
-----------
Shared pytest fixtures and configuration for the IoT pipeline test suite.
"""

import sys
from pathlib import Path

# Ensure project modules are importable from any test
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT / "dags"))
