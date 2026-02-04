-- Create a view over the silver prices parquet partitions.
-- Run this in DuckDB from the repo root so the relative path resolves.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.prices_parquet AS
SELECT *
FROM read_parquet('data/silver/prices/date=*/prices.parquet', hive_partitioning = true);
