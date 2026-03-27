import os
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster import Field, Int, String, resource


def resolve_duckdb_path(
    configured_path: str | None = None,
    *,
    env_var: str = "PORTFOLIO_DUCKDB_PATH",
    default_db_name: str = "portfolio.duckdb",
) -> Path:
    env_path = os.getenv(env_var)
    if configured_path:
        return Path(configured_path)
    if env_path:
        return Path(env_path)
    data_root = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
    return data_root / "duckdb" / default_db_name


def duckdb_lock_path_for(db_path: Path) -> Path:
    return db_path.parent / f".{db_path.name}.write.lock"


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
                pid_text = path.read_text(encoding="ascii").strip()
                lock_pid = int(pid_text)
                if lock_pid != os.getpid() and not _pid_is_running(lock_pid):
                    path.unlink()
                    continue
            except (FileNotFoundError, ValueError, OSError, SystemError):
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


def _is_file_in_use_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "cannot open file" in message and "being used by another process" in message


@resource(
    config_schema={
        "db_path": Field(String, is_required=False),
        "env_var": Field(String, is_required=False, default_value="PORTFOLIO_DUCKDB_PATH"),
        "default_db_name": Field(String, is_required=False, default_value="portfolio.duckdb"),
        "lock_timeout_seconds": Field(Int, is_required=False, default_value=120),
        "stale_lock_seconds": Field(Int, is_required=False, default_value=600),
    }
)
def duckdb_resource(context):
    configured_path = context.resource_config.get("db_path")
    env_var = context.resource_config.get("env_var", "PORTFOLIO_DUCKDB_PATH")
    default_db_name = context.resource_config.get("default_db_name", "portfolio.duckdb")
    db_path = resolve_duckdb_path(
        configured_path,
        env_var=env_var,
        default_db_name=default_db_name,
    )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = duckdb_lock_path_for(db_path)

    lock_timeout_seconds = context.resource_config.get("lock_timeout_seconds", 120)
    stale_lock_seconds = context.resource_config.get("stale_lock_seconds", 600)

    lock_fd = _acquire_duckdb_lock(
        lock_path,
        timeout_seconds=lock_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )
    connection = None
    try:
        try:
            connection = duckdb.connect(str(db_path))
        except duckdb.IOException as exc:
            if _is_file_in_use_error(exc):
                raise RuntimeError(
                    "DuckDB database is already open in another process. "
                    f"Close any external DuckDB sessions for {db_path} and retry."
                ) from exc
            raise
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
