-- Create a view over the silver factors parquet file.
-- Run this in DuckDB from the repo root so the relative path resolves.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.vw_factors AS
SELECT *
FROM read_parquet('data/silver/factors/factors.parquet');
