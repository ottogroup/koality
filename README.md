<p align="center">
  <img src="docs/assets/logo.png" alt="Koality Logo" width="400">
</p>

<p align="center">
  <strong>Data Quality Monitoring powered by DuckDB</strong>
</p>

<p align="center">
  <a href="https://github.com/ottogroup/koality/actions/workflows/tests.yml"><img src="https://github.com/ottogroup/koality/actions/workflows/tests.yml/badge.svg" alt="Tests"></a>
  <a href="https://github.com/ottogroup/koality/actions/workflows/release.yml"><img src="https://github.com/ottogroup/koality/actions/workflows/release.yml/badge.svg" alt="Release"></a>
  <a href="https://github.com/ottogroup/koality/actions/workflows/pages/pages-build-deployment"><img src="https://github.com/ottogroup/koality/actions/workflows/pages/pages-build-deployment/badge.svg" alt="Pages Deployment"></a>
  <a href="https://codecov.io/gh/ottogroup/koality"><img src="https://codecov.io/gh/ottogroup/koality/branch/main/graph/badge.svg" alt="Codecov"></a>
  <a href="https://pypi.org/project/koality/"><img src="https://img.shields.io/pypi/v/koality.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/koality/"><img src="https://img.shields.io/pypi/status/koality.svg" alt="PyPI status"></a>
  <a href="https://pypi.org/project/koality/"><img src="https://img.shields.io/pypi/pyversions/koality.svg" alt="Python versions"></a>
  <a href="https://pepy.tech/project/koality"><img src="https://static.pepy.tech/personalized-badge/koality?period=month&units=international_system&left_color=grey&right_color=blue&left_text=PyPI%20downloads/month" alt="PyPI downloads"></a>
  <a href="https://github.com/ottogroup/koality/blob/main/LICENSE.md"><img src="https://img.shields.io/github/license/ottogroup/koality" alt="License"></a>
</p>

---

**Koality** is a Python library for data quality monitoring (DQM) using [DuckDB](https://duckdb.org/). It provides configurable checks that validate data in tables and can persist results to database tables for monitoring and alerting.

We would like to thank [Norbert Maager](https://github.com/norbertmaager) who is the original inventor of Koality.

**Warning**

This library is a work in progress!

Breaking changes should be expected until a 1.0 release, so version pinning is recommended.

## Documentation

For comprehensive documentation, visit the [Koality Documentation](https://ottogroup.github.io/koality/latest).

## Core Features

- **Configurable Checks**: Define data quality checks via simple YAML configuration files
- **DuckDB-Powered**: Fast, in-process analytics with DuckDB's in-memory engine
- **External Database Support**: Currently supports Google Cloud BigQuery via DuckDB extensions
- **Multiple Check Types**: Null ratios, regex matching, value sets, duplicates, counts, match rates, outlier detection, and more
- **Flexible Filtering**: Dynamic filtering system with column/value pairs for targeted checks
- **Result Persistence**: Store check results in database tables for historical tracking
- **CLI Tool**: Easy-to-use command-line interface for running checks
- **Threshold Validation**: Compare check results against configurable lower/upper bounds

## Supported Databases

| Database              | Status            |
|-----------------------|-------------------|
| DuckDB (in-memory)    | ✅ Fully supported |
| Google Cloud BigQuery | ✅ Fully supported |

Koality uses DuckDB as its query engine. External databases are accessed through DuckDB extensions (e.g., the BigQuery extension for Google Cloud).
External databases may need custom handling in [`execute_query`](src/koality/utils.py)!

## Available Checks

| Check Type                | Description                                  |
|---------------------------|----------------------------------------------|
| `NullRatioCheck`          | Share of NULL values in a column             |
| `RegexMatchCheck`         | Share of values matching a regex pattern     |
| `ValuesInSetCheck`        | Share of values matching a predefined set    |
| `RollingValuesInSetCheck` | Values in set over a rolling time window     |
| `DuplicateCheck`          | Number of duplicate values in a column       |
| `CountCheck`              | Row count or distinct value count            |
| `AverageCheck`            | Average of a column                          |
| `MaxCheck`                | Maximum of a column                          |
| `MinCheck`                | Minimum of a column                          |
| `MatchRateCheck`          | Match rate between two tables after joining  |
| `RelCountChangeCheck`     | Relative count change vs. historical average |
| `IqrOutlierCheck`         | Detect outliers using interquartile range    |
| `OccurrenceCheck`         | Check value occurrence frequency             |

## Installation

```bash
pip install koality
```

Or add to your `pyproject.toml`:

```toml
[project]
dependencies = [
    "koality>=0.1.0",
]
```

## Quick Start

### 1. Create a configuration file

```yaml
# koality_config.yaml
name: My Data Quality Checks

# Database connection setup - executed before running checks
database_setup: |
  INSTALL bigquery;
  LOAD bigquery;
  ATTACH 'project=${PROJECT_ID}' AS bq (TYPE bigquery, READ_ONLY);

# Prefix for table references (use attached database name)
database_accessor: bq

defaults:
  result_table: bq.dqm.results
  log_path: dqm_failures.txt
  filters:
    partition_date:
      column: date
      value: yesterday
      type: date

check_bundles:
  - name: null_ratio_checks
    defaults:
      check_type: NullRatioCheck
      table: bq.dataset.orders
      lower_threshold: 0
      upper_threshold: 0.05
    checks:
      - check_column: customer_id
      - check_column: order_date
      - check_column: total_amount
```

For in-memory DuckDB (local testing or CSV/Parquet files):

```yaml
database_setup: |
  CREATE TABLE orders AS SELECT * FROM 'data/orders.parquet';
  CREATE TABLE results (check_name VARCHAR, result DOUBLE, timestamp TIMESTAMP);
database_accessor: ""
```

### 2. Run checks via CLI

```bash
# Pass database setup variables via CLI
koality run --config_path koality_config.yaml -dsv PROJECT_ID=my-gcp-project

# Or via environment variable
DATABASE_SETUP_VARIABLES="PROJECT_ID=my-gcp-project" koality run --config_path koality_config.yaml
```

### 3. Review results

Results are persisted to your configured result table and failures are logged to the specified log path.

## Configuration Hierarchy

Koality uses a hierarchical configuration system where more specific settings override general ones:

1. **`defaults`**: Base settings for all checks (result table, persistence, filters)
2. **`check_bundles.defaults`**: Bundle-level defaults (check type, table, thresholds)
3. **`checks`**: Individual check configurations (specific columns, custom thresholds)

## Filter System

Apply dynamic filters to check specific data subsets using the structured `filters` syntax:

```yaml
defaults:
  filters:
    partition_date:
      column: created_at
      value: yesterday
      type: date           # Required for rolling checks; auto-parses date values
    shop_id:
      column: shop_id
      value: SHOP01
      type: identifier     # Marks this as the identifier filter for result grouping
    revenue:
      column: total_revenue
      value: 1000
      operator: ">="       # Supports =, !=, >, >=, <, <=, IN, NOT IN, LIKE, NOT LIKE
```

### Filter Properties

| Property        | Description                                                                    |
|-----------------|--------------------------------------------------------------------------------|
| `column`        | Database column name to filter on (optional in defaults, required after merge) |
| `value`         | Filter value (optional in defaults, required after merge)                      |
| `type`          | `date`, `identifier`, or `other` (default). Only one of each type allowed      |
| `operator`      | SQL operator: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `LIKE`          |
| `parse_as_date` | If `true`, parse value as date (for non-date-type filters)                     |

## Date Parsing

Koality automatically parses date values when `type: date` is set:

- **Relative dates**: `today`, `yesterday`, `tomorrow`
- **ISO dates**: `2024-01-15`, `20240115`
- **With inline offset**: `yesterday-2` (2 days before yesterday), `today+1` (tomorrow)

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests on [GitHub](https://github.com/ottogroup/koality).

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.