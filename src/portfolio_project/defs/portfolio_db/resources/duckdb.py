import os
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster import Field, Int, String, resource


def _acquire_duckdb_lock(
    path: Path,
    timeout_seconds: int = 120,
    poll_seconds: float = 1.0,
    stale_lock_seconds: int = 600,
) -> int:
    def _pid_is_running(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            # signal 0 checks for process existence without sending a signal
            os.kill(pid, 0)
        except PermissionError:
            return True
        except (OSError, SystemError, ValueError):
            return False
        return True

    start = time.time()
    while True:
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("ascii"))
            return fd
        except FileExistsError:
            try:
                # If the recorded PID is not running, treat the lock as stale.
                pid_text = path.read_text(encoding="ascii").strip()
                lock_pid = int(pid_text)
                if lock_pid != os.getpid() and not _pid_is_running(lock_pid):
                    path.unlink()
                    continue
            except (FileNotFoundError, ValueError, OSError, SystemError):
                # If we cannot read the PID, fall back to age-based staleness.
                pass
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
        try:
            lock_pid = int(path.read_text(encoding="ascii").strip())
        except (FileNotFoundError, ValueError, OSError):
            lock_pid = None
        if lock_pid == os.getpid():
            try:
                path.unlink()
            except FileNotFoundError:
                pass


class LockedDuckDBConnection:
    """
    Thin DuckDB proxy that acquires the file lock for each DB operation.

    This prevents holding the lock during non-DB work (for example API calls),
    while still serializing write/read statements across processes.
    """

    def __init__(
        self,
        connection,
        lock_path: Path,
        lock_timeout_seconds: int,
        stale_lock_seconds: int,
    ) -> None:
        self._connection = connection
        self._lock_path = lock_path
        self._lock_timeout_seconds = lock_timeout_seconds
        self._stale_lock_seconds = stale_lock_seconds

    @contextmanager
    def _operation_lock(self):
        lock_fd = _acquire_duckdb_lock(
            self._lock_path,
            timeout_seconds=self._lock_timeout_seconds,
            stale_lock_seconds=self._stale_lock_seconds,
        )
        try:
            yield
        finally:
            _release_duckdb_lock(self._lock_path, lock_fd)

    def execute(self, query, parameters=None):
        with self._operation_lock():
            if parameters is None:
                return self._connection.execute(query)
            return self._connection.execute(query, parameters)

    def executemany(self, query, parameters=None):
        with self._operation_lock():
            if parameters is None:
                return self._connection.executemany(query)
            return self._connection.executemany(query, parameters)

    def register(self, view_name, python_object):
        with self._operation_lock():
            return self._connection.register(view_name, python_object)

    def commit(self):
        with self._operation_lock():
            return self._connection.commit()

    def close(self):
        return self._connection.close()

    def __getattr__(self, name):
        return getattr(self._connection, name)


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

    lock_timeout_seconds = context.resource_config.get("lock_timeout_seconds", 120)
    stale_lock_seconds = context.resource_config.get("stale_lock_seconds", 600)

    # Serialize connection creation and guarantee lock release if connect fails.
    lock_fd = _acquire_duckdb_lock(
        lock_path,
        timeout_seconds=lock_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )
    connection = None
    try:
        connection = duckdb.connect(str(db_path))
    finally:
        _release_duckdb_lock(lock_path, lock_fd)

    wrapped_connection = LockedDuckDBConnection(
        connection=connection,
        lock_path=lock_path,
        lock_timeout_seconds=lock_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )
    try:
        yield wrapped_connection
    finally:
        wrapped_connection.close()
