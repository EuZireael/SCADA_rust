"""pytest: модули симулятора лежат в simulator/ (пакеты core.*, модуль sim_records)."""
import sys
from pathlib import Path

SIM = str(Path(__file__).resolve().parent.parent)
if SIM not in sys.path:
    sys.path.insert(0, SIM)
