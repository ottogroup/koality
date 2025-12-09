# Welcome to Koality

Koality is a Python library for data quality checks and monitoring based on DuckDB. It provides a flexible framework for validating data in tables, detecting anomalies, and ensuring data quality across your data pipelines.

## Features

- **Multiple Check Types**: Null ratio, regex matching, value sets, duplicates, counts, match rates, and more
- **YAML Configuration**: Define checks declaratively in YAML files with global defaults and check bundles
- **DuckDB Backend**: Fast, in-process analytical database for executing checks
- **CLI Interface**: Run checks from the command line with `koality`
- **Extensible**: Easy to add custom check types by extending base classes
- **Result Persistence**: Store check results in database tables for historical tracking

## Quick Start

```python
from koality import CheckExecutor
from koality.models import Config
from pydantic_yaml import parse_yaml_raw_as

# Load configuration from YAML
with open("checks.yaml") as f:
    config = parse_yaml_raw_as(Config, f.read())

# Execute checks
executor = CheckExecutor(config)
results = executor()

# Check for failures
if executor.check_failed:
    print(executor.get_failed_checks_msg())
```

## Installation

```bash
pip install koality
```

Or with uv:

```bash
uv add koality
```

## Available Checks

| Check                                                                                    | Description                                    |
|------------------------------------------------------------------------------------------|------------------------------------------------|
| [`NullRatioCheck`](machinery/checks.md#koality.checks.NullRatioCheck)                    | Validates the ratio of null values in a column |
| [`RegexMatchCheck`](machinery/checks.md#koality.checks.RegexMatchCheck)                  | Checks if values match a regex pattern         |
| [`ValuesInSetCheck`](machinery/checks.md#koality.checks.ValuesInSetCheck)                | Validates values are within an allowed set     |
| [`RollingValuesInSetCheck`](machinery/checks.md#koality.checks.RollingValuesInSetCheck)  | Rolling window version of ValuesInSetCheck     |
| [`DuplicateCheck`](machinery/checks.md#koality.checks.DuplicateCheck)                    | Detects duplicate values in a column           |
| [`CountCheck`](machinery/checks.md#koality.checks.CountCheck)                            | Validates row counts or distinct counts        |
| [`OccurrenceCheck`](machinery/checks.md#koality.checks.OccurrenceCheck)                  | Checks min/max occurrence of values            |
| [`MatchRateCheck`](machinery/checks.md#koality.checks.MatchRateCheck)                    | Validates join match rates between tables      |
| [`RelCountChangeCheck`](machinery/checks.md#koality.checks.RelCountChangeCheck)          | Detects relative count changes over time       |
| [`IqrOutlierCheck`](machinery/checks.md#koality.checks.IqrOutlierCheck)                  | Detects outliers using interquartile range     |

## Documentation

- [Getting Started](getting-started.md) - Installation and first steps
- [Configuration](configuration.md) - YAML configuration reference
- [API Reference](machinery/index.md) - Full API documentation