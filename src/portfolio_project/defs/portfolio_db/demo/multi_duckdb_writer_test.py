from datetime import datetime, timezone
import os

from dagster import In, Out, graph, multiprocess_executor, op


def _ensure_probe_table(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS diagnostics")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS diagnostics.multi_writer_probe (
            test_id VARCHAR,
            run_id VARCHAR,
            target_db VARCHAR,
            writer_pid BIGINT,
            wrote_at TIMESTAMP
        )
        """
    )


def _write_probe_row(con, *, test_id: str, run_id: str, target_db: str) -> None:
    _ensure_probe_table(con)
    con.execute(
        """
        INSERT INTO diagnostics.multi_writer_probe (
            test_id,
            run_id,
            target_db,
            writer_pid,
            wrote_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            test_id,
            run_id,
            target_db,
            os.getpid(),
            datetime.now(timezone.utc),
        ],
    )
    try:
        con.commit()
    except Exception:
        pass


@op(out=Out(str))
def build_multi_writer_test_id(context) -> str:
    test_id = f"{context.run_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    context.log.info("Starting multi-DB writer smoke test: %s", test_id)
    return test_id


@op(required_resource_keys={"duckdb"}, ins={"test_id": In(str)}, out=Out(str))
def write_portfolio_probe(context, test_id: str) -> str:
    _write_probe_row(
        context.resources.duckdb,
        test_id=test_id,
        run_id=context.run_id,
        target_db="portfolio",
    )
    context.log.info("Wrote multi-writer probe row to portfolio DuckDB for test_id=%s", test_id)
    return test_id


@op(required_resource_keys={"research_duckdb"}, ins={"test_id": In(str)}, out=Out(str))
def write_research_probe(context, test_id: str) -> str:
    _write_probe_row(
        context.resources.research_duckdb,
        test_id=test_id,
        run_id=context.run_id,
        target_db="research",
    )
    context.log.info("Wrote multi-writer probe row to research DuckDB for test_id=%s", test_id)
    return test_id


@op(
    required_resource_keys={"duckdb", "research_duckdb"},
    ins={"portfolio_test_id": In(str), "research_test_id": In(str)},
)
def verify_multi_writer_probe(context, portfolio_test_id: str, research_test_id: str) -> None:
    if portfolio_test_id != research_test_id:
        raise ValueError(
            f"Mismatched multi-writer test ids: portfolio={portfolio_test_id}, research={research_test_id}"
        )

    portfolio_count = context.resources.duckdb.execute(
        "SELECT count(*) FROM diagnostics.multi_writer_probe WHERE test_id = ? AND target_db = 'portfolio'",
        [portfolio_test_id],
    ).fetchone()[0]
    research_count = context.resources.research_duckdb.execute(
        "SELECT count(*) FROM diagnostics.multi_writer_probe WHERE test_id = ? AND target_db = 'research'",
        [research_test_id],
    ).fetchone()[0]

    if int(portfolio_count or 0) < 1:
        raise RuntimeError(f"Portfolio DuckDB probe row missing for test_id={portfolio_test_id}")
    if int(research_count or 0) < 1:
        raise RuntimeError(f"Research DuckDB probe row missing for test_id={research_test_id}")

    context.log.info(
        "Verified multi-writer probe rows in both DuckDB databases for test_id=%s",
        portfolio_test_id,
    )


@graph
def multi_duckdb_writer_smoke_test_graph() -> None:
    test_id = build_multi_writer_test_id()
    portfolio_test_id = write_portfolio_probe(test_id)
    research_test_id = write_research_probe(test_id)
    verify_multi_writer_probe(portfolio_test_id, research_test_id)


multi_duckdb_writer_smoke_test_job = multi_duckdb_writer_smoke_test_graph.to_job(
    name="multi_duckdb_writer_smoke_test_job",
    executor_def=multiprocess_executor,
)
