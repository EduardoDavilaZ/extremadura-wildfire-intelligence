import sys
from pathlib import Path

_PKG_PREFIXES = ("src.", "bronze.", "silver.", "gold.", "config.")
_PKG_NAMES = {"src", "bronze", "silver", "gold", "config"}


def project_root() -> Path:
    start = Path.cwd().resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "src").is_dir() and (candidate / "config").is_dir():
            return candidate
    raise FileNotFoundError("No se encontró la raíz del proyecto (src/ y config/).")


def ensure_project_on_path() -> Path:
    root = project_root()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root


def reload_src_packages() -> None:
    # Invalida imports cacheados en Databricks al re-ejecutar el notebook.
    for name in list(sys.modules):
        if name in _PKG_NAMES or name.startswith(_PKG_PREFIXES):
            del sys.modules[name]
