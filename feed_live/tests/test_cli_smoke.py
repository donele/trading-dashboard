import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


def test_dash_help_exits_zero() -> None:
    proc = _run("-m", "dashboard.shm_dash_app", "--help")
    assert proc.returncode == 0, proc.stderr
    assert "Dash app for live SHM prices" in proc.stdout


def test_reader_help_exits_zero() -> None:
    proc = _run("-m", "dashboard.shm_direct_price_reader", "--help")
    assert proc.returncode == 0, proc.stderr
    assert "Direct Python SHM price reader" in proc.stdout
