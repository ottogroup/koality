# koality

This repository contains koality - a library for checks on big query tables for data quality monitoring (DQM). It contains a number of generic data quality checks (e.g., null ratio checks, match rates) which can be used to check specific situations of different projects (i.e., actual tables and columns).

The library provides the command-line tool *koality* which can be used - in combination with a koality configuration yaml - to perform checks and to log their results.

## Installation

### Using pyproject.toml

```toml
[project]
# ...
dependencies = [
    # ...
    "koality>=x.y.z",
]
```

## Using the CLI

For running koality with a specific configuration, you need to provide the path to the config e.g.:

```shell
koality --config_path config/foo.yaml
```

It is also possible to overwrite some global defaults of a configuration (which are no explicit parameters of the CLI command). This can be handy if you want to test a configuration in a production setting without storing actual results in the production DQM monitoring table:

```shell
# Check prod without persisting results
koality --config_path config/foo.yaml
```

## Configuration files

Configuration files contain different structure levels where more specific sections overrule parameters of the more generic sections. The different structure levels are:

- `global_defaults`: Values that should be used globally, e.g., if and where results should be persisted.
- `check_bundles`: Checks that have something in common, e.g., the same check type on a specific column. A bundle consists of `default_args` and a list of checks.
- `checks`: Most specific level, e.g., individual checks for different `shop_ids`. Each check can overrule parameters specified by its check bundle or by the global defaults.

### Global parameters

The global parameters specify the generic DQM handling:

- `monitor_only`: If true, results are monitored only, i.e., it is not checked if results are within specified ranges.
- `result_table`: BigQuery table where results are stored.
- `persist_results`: Flag if results should be stored in BQ.
- `log_path`: Path to file where failed checks are logged. Logs will be written to this path, independent of
              `persist_results`. However, if `monitor_only` is active, no checks will be performed (and thus, no
              failed checks will be logged).

You can also specify additional global parameters to be used in all your checks as default, e.g., date filter or shop filter specifications.

### Example configuration file

```yaml
# koality_example_config.yaml

name: Dataquality Monitoring Example Config

global_defaults:
  date_filter_column: date
  shop_id_filter_column: shopId
  monitor_only: False
  result_table: project.dataset.koality
  persist_results: True
  log_path: message.txt
  date: yesterday

check_bundles:
  - name: margin_null_ratio
    default_args:
      check_type: NullRatioCheck
      table: project.dataset.table
      check_column: bar
      lower_threshold: 0
      upper_threshold: 0.05
      date_info: "uses data of previous day"
    checks:
      - shop_id: SHOP01
        extra_info: "Note: No margin value for some brands."
      - shop_id: SHOP02
```

In this example, the `global_defaults` specify generic DQM handling like if and where to persist results and where to log failed checks. It also provides global filter information for date and shop (i.e., if a check should be run for a specific day and a specific shop, and thus, data need to be filtered correspondingly).

## Checks

Checks execute a check query, usually on one or two tables, resulting in a check value (e.g., counts, null ratio or match rate). It is then checked if the result value is within specified bounds (lower/upper thresholds). If this is not the case, the check will fail and all failed checks will be logged.

The following check types are supported:

- `NullRatioCheck`: Checks the share of NULL values in a specific column of a table.
- `RegexMatchCheck`: Checks the share of values matching a regex in a specific column of a table.
- `ValuesInSetCheck`: Checks the share of values that match any value of a value set in a specific column of a table.
- `RollingValuesInSetCheck`: Similar to `ValuesInSetCheck`, but the share is computed for a longer time period (currently also including data of the 14 days before the actual check date).
- `DuplicateCheck`: Checks the number of duplicates for a specific column, i.e., all counts - distinct counts.
- `CountCheck`: Checks the number of rows or distinct values of a specific column.
- `MatchRateCheck`: Checks the match rate between two tables after joining on specific columns.
- `RelCountChangeCheck`: Checks the relative change of a count in comparison to the average counts of a number of historic days before the check date.
- `IqrOutlierCheck`: Checks if the date-specific value of a specific column is within the interquartile range (IQR) of values of previous x days.
- `OccurenceCheck`: Checks if *any* value in a column occurs more / less often than specified thresholds.

As shown in the example configuration above, it is also possible to provide some additional information about the date in the `date_info` argument or about the check in general in the `extra_info` argument. In case of failed checks, these texts will be added to the failure message.

For further details take a look at the `checks` module.

As the idea of this package is to provide a flexible and extensible tool for various situations, feel free to implement new checks as needed.

### Filters

koality contains a flexible filter logic which can be used to take into account only a subset of the data for a specific check. The most obvious filters are date and shop filters as in many cases predictions are performed for a specific day / shop combination.

In order to specify such filters, you have to use the `_filter_column` postfix to corresponding parameters. Filter values have to be specified by another parameter with the same prefix (or with same prefix and `_filter_value` postfix), e.g.:

```
date_filter_column: date
date: yesterday
shop_id_filter_column: shopId
shop_id: shopId
```

### Time magic

koality also supports some date strings and relative dates in configurations:

- `today`
- `yesterday`
- `tomorrow`
- `today-2`
- `today+3`

## Result tables

The resulting table contains all relevant information of koality checks:

| DATE       |  METRIC_NAME        | SHOP_ID | TABLE                 |  COLUMN        |  VALUE |  LOWER_THRESHOLD |  UPPER_THRESHOLD |  RESULT |
|------------|---------------------|---------|-----------------------|----------------|--------|------------------|------------------|---------|
| 2023-08-09 | category_null_ratio | SHOP01  | project.dataset.table | value.category |  0.065 |              0.0 |             0.06 | FAIL    |
| 2023-08-09 | category_null_ratio | SHOP02  | project.dataset.table | value.category |  0.077 |              0.0 |             0.12 | SUCCESS |
| 2023-08-09 | row_count           | SHOP03  | project.dataset.table | *              | 5226.0 |            100.0 |         Infinity | SUCCESS |

## Releasing a new version

New versions of koality can be released via GitHub UI. Make sure that the version number in the `pyproject.toml` file is updated to the new version you want to release and that all changes are merged to the `main` branch. Then you can add a new release in the releases section of your repository. Once a new release tag is added, the `.github/workflows/release.yml` GitHub action workflow will be triggered and the new release will be deployed to the artifact registry.

## Patching
- If you want to deploy new version, increase the version number in `pyproject.toml`
- Update package version in [pyproject.toml](pyproject.toml)
- Run `uv lock --upgrade`
- Update github actions versions