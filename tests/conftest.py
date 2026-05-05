import sys
import pathlib

root = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(str(root / "src"))
