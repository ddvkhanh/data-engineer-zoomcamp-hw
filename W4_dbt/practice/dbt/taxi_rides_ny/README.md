## Local Setup
1. Create and activate a Python virtual environment.
2. Install required dependencies:
    - dbt-duckdb (dbt adapter for DuckDB)
    - DuckDB CLI (via Homebrew)
3. Configure dbt by editing ~/.dbt/profiles.yml to point to the local DuckDB database.
4. dbt init
Add and run the ingestion script to create and populate the DuckDB database.
5. Launch DuckDB UI (duckdb --ui), copy the path to taxi_rides_ny.duckdb, and register it as a local database.
6. Verify the setup by running dbt debug.


## Question 1
Given a dbt project with the following structure:
```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```
If you run dbt run --select int_trips_unioned, what models will be built?

- stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)
- Any model with upstream and downstream dependencies to int_trips_unioned
- int_trips_unioned only
- int_trips_unioned, int_trips, and fct_trips (downstream dependencies)

### Answer: 
`int_trips_unioned only`

**Explain**
dbt run --select will only build the 1 model selected, if we want to build upstream or downstream, add "+" to the model. E.g: `dbt run --select +int_trips_unioned`

## Question 2
You've configured a generic test like this in your schema.yml:
```
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```
Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data.

What happens when you run dbt test --select fct_trips?

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

### Answer: 


**Explain**

