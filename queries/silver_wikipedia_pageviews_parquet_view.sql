-- Create a view over the silver wikipedia pageviews parquet partitions.
-- Run this in DuckDB from the repo root so the relative path resolves.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.vs_wikipedia_pageviews AS
SELECT *
FROM read_parquet('data/silver/wikipedia_pageviews/date=*/pageviews.parquet', hive_partitioning = true);
