from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import Final, List, Sequence

PROJECT_ROOT: Final[Path] = Path(__file__).parent.parent
TESTS_DIR: Final[Path] = PROJECT_ROOT / "tests"
SOURCE_DIR: Final[Path] = PROJECT_ROOT / "genericache"

@dataclass
class PyVersion:
    major: int
    minor: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"


SUPPORTED_PYTHON_VERSIONS: Final[List[PyVersion]] = [
    PyVersion(major=3, minor=9),
    PyVersion(major=3, minor=10),
    PyVersion(major=3, minor=11),
    PyVersion(major=3, minor=12),
    PyVersion(major=3, minor=13),
]

def uv_run(*, py_ver: PyVersion, no_dev: bool, command: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run([
        "uv",
        "run",
        *(["--no-dev"] if no_dev else []),
        f"--python={py_ver}",
        *command
    ])

def pyright_check(*, py_ver: PyVersion, no_dev: bool, directory: Path) -> subprocess.CompletedProcess[bytes]:
    return uv_run(
        py_ver=py_ver,
        no_dev=no_dev,
        command=["npx", "pyright", f"--pythonversion={py_ver}", str(directory)]
    )
