# Getting Started

This guide will help you get started with Koality for data quality monitoring.

## Installation

### Using pip

```bash
pip install koality
```

### Using uv

```bash
uv add koality
```

## Supported Databases

Koality uses DuckDB as its query engine. Currently supported:

- **DuckDB (in-memory)** - Fully supported
- **Google Cloud BigQuery** - Fully supported via DuckDB's BigQuery extension

Other databases may work out of the box but may also be supported in future releases through extension of the 
[`execute_query`](machinery/utils.md) function. Contributions are welcome!

## Basic Usage

### 1. Create a Configuration File

Create a YAML configuration file (`checks.yaml`) that defines your data quality checks:

```yaml
name: my_dqm_checks
database_setup: |
  ATTACH 'my_database.duckdb' AS my_db;
database_accessor: my_db

defaults:
  result_table: dqm_results
  filters:
    partition_date:
      column: date
      value: "2024-01-01"
      type: date

check_bundles:
  - name: orders_checks
    defaults:
      table: orders
    checks:
      - check_type: NullRatioCheck
        check_column: order_id
        lower_threshold: 0.0
        upper_threshold: 0.0

      - check_type: CountCheck
        check_column: "*"
        lower_threshold: 1000
        upper_threshold: 1000000
```

#### Understanding database_setup and database_accessor

- **`database_setup`**: SQL commands executed when the CheckExecutor initializes. Use this to install extensions, attach databases, and configure connections.
- **`database_accessor`**: The alias/name of your attached database. Koality uses this to identify the database type and route queries appropriately.

**Example for Google Cloud BigQuery:**

```yaml
name: bigquery_checks
database_setup: |
  INSTALL bigquery;
  LOAD bigquery;
  ATTACH 'project=my-gcp-project' AS bq (TYPE bigquery, READ_ONLY);
database_accessor: bq
```

When using BigQuery, Koality automatically uses the BigQuery Jobs API for reads (which supports views) and `bigquery_execute()` for writes.

### 2. Run Checks Programmatically

```python
from koality import CheckExecutor
from koality.models import Config
from pydantic_yaml import parse_yaml_raw_as

# Load configuration
with open("checks.yaml") as f:
    config = parse_yaml_raw_as(Config, f.read())

# Create executor and run checks
executor = CheckExecutor(config)
results = executor()

# Handle results
if executor.check_failed:
    print("Some checks failed:")
    print(executor.get_failed_checks_msg())
else:
    print("All checks passed!")
```

### 3. Run Checks via CLI

```bash
koality run --config_path checks.yaml
```

#### Override Configuration at Runtime

Use the `--overwrites` (`-o`) option to override configuration values without modifying the YAML file:

```bash
# Override a filter value (e.g., run for a specific date instead of "yesterday")
koality run --config_path checks.yaml -o defaults.filters.partition_date=2023-06-15

# Override multiple values
koality run --config_path checks.yaml -o defaults.filters.partition_date=2023-06-15 -o defaults.filters.shop_id=SHOP02

# Override other defaults
koality run --config_path checks.yaml -o defaults.identifier_format=column_name -o defaults.monitor_only=true

# Override filter fields (column, operator, etc.)
koality run --config_path checks.yaml -o defaults.filters.partition_date.column=OTHER_DATE_COL

# Override at bundle level (only affects that bundle)
koality run --config_path checks.yaml -o check_bundles.orders_checks.filters.partition_date=2023-06-15

# Override at check level (only affects specific check by index)
koality run --config_path checks.yaml -o check_bundles.orders_checks.0.table=orders_archive
```

Overwrites applied to global defaults propagate to all checks automatically.

### 4. Validate Configuration

Validate your configuration file without executing checks:

```bash
koality validate --config_path checks.yaml
```

### 5. Print Resolved Configuration

View the fully resolved configuration (after default propagation) in different formats:

```bash
# YAML format (default)
koality print --config_path checks.yaml

# JSON format
koality print --config_path checks.yaml --format json

# Pydantic model representation
koality print --config_path checks.yaml --format model

# Custom indentation
koality print --config_path checks.yaml --format yaml --indent 4

# Preview configuration with overwrites applied
koality print --config_path checks.yaml -o partition_date=2023-06-15
```

Using `print` with overwrites is useful for verifying your overrides are applied correctly before running checks.

## Understanding Check Results

Each check returns a result dictionary with the following fields:

| Field             | Description                                                      |
|-------------------|------------------------------------------------------------------|
| `DATE`            | The date the check was run for                                   |
| `METRIC_NAME`     | Name of the metric/check                                         |
| `IDENTIFIER`      | Identifier value (format depends on `identifier_format` setting) |
| `TABLE`           | Table being checked                                              |
| `COLUMN`          | Column being checked                                             |
| `VALUE`           | Actual value measured                                            |
| `LOWER_THRESHOLD` | Lower threshold for passing                                      |
| `UPPER_THRESHOLD` | Upper threshold for passing                                      |
| `RESULT`          | `SUCCESS`, `FAIL`, `MONITOR_ONLY`, or `ERROR`                    |

## Next Steps

- Learn about [Configuration](configuration.md) options
- Explore available [Checks](machinery/checks.md)
- See the [API Reference](machinery/index.md) for detailed documentation