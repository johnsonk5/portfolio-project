from dagster import build_resources

from duckdb_resource import duckdb_resource

with build_resources(
    {"duckdb": duckdb_resource.configured({"db_path": "data/test/portfolio_test.duckdb"})}
) as resources:
    con = resources.duckdb
    con.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
    con.execute("INSERT INTO t VALUES (1)")
    out = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    print("count =", out)