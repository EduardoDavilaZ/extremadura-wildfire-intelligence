import sys
from pathlib import Path

for p in [Path.cwd().resolve(), *Path.cwd().resolve().parents]:
    if (p / "src").is_dir():
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
        break

for m in [k for k in sys.modules if k == "src" or k.startswith(("src.", "bronze.", "silver.", "gold.", "config."))]:
    del sys.modules[m]