import os
import subprocess
import sys


def alembic(url, *arguments):
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        env=os.environ | {"DATABASE_URL": url},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout + result.stderr
