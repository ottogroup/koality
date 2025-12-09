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

## Basic Usage

### 1. Create a Configuration File

Create a YAML configuration file (`checks.yaml`) that defines your data quality checks:

```yaml
name: my_dqm_checks
database_setup: |
  ATTACH 'my_database.duckdb' AS my_db;
database_accessor: my_db

defaults:
  monitor_only: false
  result_table: dqm_results
  date_filter_column: date
  date_filter_value: "2024-01-01"

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
koality run checks.yaml
```

## Understanding Check Results

Each check returns a result dictionary with the following fields:

| Field | Description |
|-------|-------------|
| `DATE` | The date the check was run for |
| `METRIC_NAME` | Name of the metric/check |
| `SHOP_ID` | Shop identifier (if using shop filters) |
| `TABLE` | Table being checked |
| `COLUMN` | Column being checked |
| `VALUE` | Actual value measured |
| `LOWER_THRESHOLD` | Lower threshold for passing |
| `UPPER_THRESHOLD` | Upper threshold for passing |
| `RESULT` | `SUCCESS`, `FAIL`, `MONITOR_ONLY`, or `ERROR` |

## Next Steps

- Learn about [Configuration](configuration.md) options
- Explore available [Checks](machinery/checks.md)
- See the [API Reference](machinery/index.md) for detailed documentation