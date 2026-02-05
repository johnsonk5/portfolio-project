-- Create a view over the silver news parquet partitions.
-- Run this in DuckDB from the repo root so the relative path resolves.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.vw_news AS
SELECT *
FROM read_parquet('data/silver/news/date=*/news.parquet', hive_partitioning = true);
