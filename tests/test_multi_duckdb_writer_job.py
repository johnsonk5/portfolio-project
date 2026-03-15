from pathlib import Path

import duckdb

from portfolio_project.defs.portfolio_db.demo.multi_duckdb_writer_test import (
    multi_duckdb_writer_smoke_test_job,
)
from portfolio_project.defs.portfolio_db.resources.duckdb import duckdb_resource, resolve_duckdb_path


def test_resolve_duckdb_path_supports_custom_research_defaults(
    tmp_path: Path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setenv("PORTFOLIO_DATA_DIR", str(data_root))
    monkeypatch.delenv("PORTFOLIO_RESEARCH_DUCKDB_PATH", raising=False)

    assert resolve_duckdb_path(
        env_var="PORTFOLIO_RESEARCH_DUCKDB_PATH",
        default_db_name="research.duckdb",
    ) == data_root / "duckdb" / "research.duckdb"


def test_multi_duckdb_writer_smoke_test_job_writes_both_databases(tmp_path: Path) -> None:
    portfolio_db_path = tmp_path / "duckdb" / "portfolio.duckdb"
    research_db_path = tmp_path / "duckdb" / "research.duckdb"

    result = multi_duckdb_writer_smoke_test_job.execute_in_process(
        resources={
            "duckdb": duckdb_resource.configured({"db_path": str(portfolio_db_path)}),
            "research_duckdb": duckdb_resource.configured({"db_path": str(research_db_path)}),
        }
    )

    assert result.success

    portfolio_con = duckdb.connect(str(portfolio_db_path), read_only=True)
    try:
        portfolio_count = portfolio_con.execute(
            "SELECT count(*) FROM diagnostics.multi_writer_probe WHERE target_db = 'portfolio'"
        ).fetchone()[0]
    finally:
        portfolio_con.close()

    research_con = duckdb.connect(str(research_db_path), read_only=True)
    try:
        research_count = research_con.execute(
            "SELECT count(*) FROM diagnostics.multi_writer_probe WHERE target_db = 'research'"
        ).fetchone()[0]
    finally:
        research_con.close()

    assert portfolio_count == 1
    assert research_count == 1
