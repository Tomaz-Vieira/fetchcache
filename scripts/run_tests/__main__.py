import subprocess
from pathlib import Path
from typing_extensions import Final

project_root: Final[Path] = Path(__file__).parent.parent.parent
test_dir: Final[Path] = project_root / "tests"

for py_ver in ("3.9", "3.10", "3.11", "3.12", "3.13"):
    for entry in test_dir.iterdir():
        if not entry.is_dir():
            continue
        if not entry.name.startswith("test_"):
            continue
        module_dotted_path = entry.relative_to(project_root).as_posix().replace("/", ".")
        result = subprocess.run(["uv", "run", f"--python={py_ver}", "python3", "-m", module_dotted_path])
        if result.returncode != 0:
            exit(result.returncode)

