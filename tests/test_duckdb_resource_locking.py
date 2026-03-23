import os
import time

import pytest
from dagster import DagsterResourceFunctionError, build_resources

import portfolio_project.defs.resources.duckdb as duckdb_resource_module
from portfolio_project.defs.resources.duckdb import (
    _acquire_duckdb_lock,
    _release_duckdb_lock,
    duckdb_lock_path_for,
    duckdb_resource,
)


def test_acquire_lock_replaces_stale_file(tmp_path) -> None:
    lock_path = tmp_path / ".duckdb_write.lock"
    lock_path.write_text("999999", encoding="ascii")
    old_ts = time.time() - 120
    os.utime(lock_path, (old_ts, old_ts))

    lock_fd = _acquire_duckdb_lock(
        lock_path,
        timeout_seconds=1,
        poll_seconds=0.01,
        stale_lock_seconds=1,
    )
    try:
        assert lock_path.exists()
        assert lock_path.read_text(encoding="ascii").strip() == str(os.getpid())
    finally:
        _release_duckdb_lock(lock_path, lock_fd)

    assert not lock_path.exists()


def test_acquire_lock_times_out_when_fresh_lock_exists(tmp_path) -> None:
    lock_path = tmp_path / ".duckdb_write.lock"
    lock_path.write_text(str(os.getpid()), encoding="ascii")

    with pytest.raises(TimeoutError):
        _acquire_duckdb_lock(
            lock_path,
            timeout_seconds=0,
            poll_seconds=0.01,
            stale_lock_seconds=600,
        )

    lock_path.unlink()


def test_duckdb_resource_releases_lock_when_connect_fails(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "duckdb" / "portfolio.duckdb"
    lock_path = duckdb_lock_path_for(db_path)

    def _boom(_):
        raise RuntimeError("connect failed")

    monkeypatch.setattr(duckdb_resource_module.duckdb, "connect", _boom)

    with pytest.raises(DagsterResourceFunctionError) as exc_info:
        with build_resources(
            {
                "duckdb": duckdb_resource.configured(
                    {
                        "db_path": str(db_path),
                        "lock_timeout_seconds": 1,
                        "stale_lock_seconds": 600,
                    }
                )
            }
        ):
            pass

    assert exc_info.value.__cause__ is not None
    assert "connect failed" in str(exc_info.value.__cause__)
    assert not lock_path.exists()


def test_duckdb_resource_uses_operation_scoped_locking(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "duckdb" / "portfolio.duckdb"
    lock_path = duckdb_lock_path_for(db_path)

    acquire_calls = 0
    original_acquire = duckdb_resource_module._acquire_duckdb_lock

    def _counting_acquire(*args, **kwargs):
        nonlocal acquire_calls
        acquire_calls += 1
        return original_acquire(*args, **kwargs)

    monkeypatch.setattr(duckdb_resource_module, "_acquire_duckdb_lock", _counting_acquire)

    with build_resources(
        {
            "duckdb": duckdb_resource.configured(
                {
                    "db_path": str(db_path),
                    "lock_timeout_seconds": 1,
                    "stale_lock_seconds": 600,
                }
            )
        }
    ) as resources:
        con = resources.duckdb
        assert not lock_path.exists()
        con.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
        assert not lock_path.exists()
        con.execute("INSERT INTO t VALUES (1)")
        assert not lock_path.exists()
        count = con.execute("SELECT count(*) FROM t").fetchone()[0]
        assert count == 1
        assert not lock_path.exists()

    # One lock acquire for connect + one per execute above.
    assert acquire_calls >= 4


def test_duckdb_lock_path_isolated_per_database(tmp_path) -> None:
    live_db_path = tmp_path / "duckdb" / "portfolio.duckdb"
    research_db_path = tmp_path / "duckdb" / "research.duckdb"
    live_db_path.parent.mkdir(parents=True, exist_ok=True)

    live_lock_path = duckdb_lock_path_for(live_db_path)
    research_lock_path = duckdb_lock_path_for(research_db_path)

    assert live_lock_path != research_lock_path

    live_lock_fd = _acquire_duckdb_lock(live_lock_path, timeout_seconds=1, poll_seconds=0.01)
    try:
        research_lock_fd = _acquire_duckdb_lock(
            research_lock_path,
            timeout_seconds=1,
            poll_seconds=0.01,
        )
        try:
            assert live_lock_path.exists()
            assert research_lock_path.exists()
        finally:
            _release_duckdb_lock(research_lock_path, research_lock_fd)
    finally:
        _release_duckdb_lock(live_lock_path, live_lock_fd)

    assert not live_lock_path.exists()
    assert not research_lock_path.exists()
