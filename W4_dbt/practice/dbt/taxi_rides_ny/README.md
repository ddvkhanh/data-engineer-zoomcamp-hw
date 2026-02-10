### Local Setup
1. Create and activate a Python virtual environment.
2. Install required dependencies:
    - dbt-duckdb (dbt adapter for DuckDB)
    - DuckDB CLI (via Homebrew)
3. Configure dbt by editing ~/.dbt/profiles.yml to point to the local DuckDB database.
4. Add and run the ingestion script to create and populate the DuckDB database.
5. Launch DuckDB UI (duckdb --ui), copy the path to taxi_rides_ny.duckdb, and register it as a local database.
6. Verify the setup by running dbt debug.