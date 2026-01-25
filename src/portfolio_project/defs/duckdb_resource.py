import os
import time
from pathlib import Path

import duckdb
from dagster import Field, Int, String, resource


def _acquire_duckdb_lock(
    path: Path,
    timeout_seconds: int = 120,
    poll_seconds: float = 1.0,
    stale_lock_seconds: int = 600,
) -> int:
    start = time.time()
    while True:
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("ascii"))
            return fd
        except FileExistsError:
            try:
                age_seconds = time.time() - path.stat().st_mtime
                if age_seconds >= stale_lock_seconds:
                    path.unlink()
                    continue
            except FileNotFoundError:
                continue
            if time.time() - start >= timeout_seconds:
                raise TimeoutError(f"Timed out waiting for DuckDB lock at {path}")
            time.sleep(poll_seconds)


def _release_duckdb_lock(path: Path, fd: int) -> None:
    try:
        os.close(fd)
    finally:
        if path.exists():
            path.unlink()


@resource(
    config_schema={
        "db_path": Field(String, is_required=False),
        "lock_timeout_seconds": Field(Int, is_required=False, default_value=120),
        "stale_lock_seconds": Field(Int, is_required=False, default_value=600),
    }
)
def duckdb_resource(context):
    """
    Dagster resource for a DuckDB connection.

    Configuration:
    - db_path: Optional explicit path to the DuckDB file.
    - PORTFOLIO_DUCKDB_PATH: Environment variable fallback.
    - PORTFOLIO_DATA_DIR: Base data directory fallback (defaults to "data").
    """
    configured_path = context.resource_config.get("db_path")
    env_path = os.getenv("PORTFOLIO_DUCKDB_PATH")
    if configured_path:
        db_path = Path(configured_path)
    elif env_path:
        db_path = Path(env_path)
    else:
        data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
        db_path = data_root / "duckdb" / "portfolio.duckdb"

    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = db_path.parent / ".duckdb_write.lock"

    lock_fd = _acquire_duckdb_lock(
        lock_path,
        timeout_seconds=context.resource_config.get("lock_timeout_seconds", 120),
        stale_lock_seconds=context.resource_config.get("stale_lock_seconds", 600),
    )
    connection = duckdb.connect(str(db_path))
    try:
        yield connection
    finally:
        connection.close()
        _release_duckdb_lock(lock_path, lock_fd)
