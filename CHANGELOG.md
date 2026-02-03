# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Types of changes:

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [Unreleased]

### Fixed

- Improve nested/dotted column handling: when data is loaded from external sources via database accessors, nested 
  columns (e.g., `value.shopId`) are now aliased with underscores (`value_shopId`) for consistent querying. Native 
  DuckDB struct columns continue to use dot notation. This ensures proper handling in WHERE clauses, JOIN conditions, 
  and SELECT statements across all check types.
- Consolidate date-type filters to be ORed together (instead of ANDed) when fetching data into memory, allowing multiple
  date conditions to apply correctly.

## [0.11.2] - 2026-01-23

### Fixed

- Add `identifier_placeholder` parameter to all missing relevant check functions and forward to constructors

## [0.11.1] - 2026-01-22

### Fixed

- Ensure all check classes accept and forward `identifier_placeholder` so naming-only identifier filters use the configured placeholder for result naming and logging. Add unit tests covering the behavior.


## [0.11.0] - 2026-01-22

### Fixed

- Ensure data loaded into DuckDB memory respects all configured filters and rolling windows (e.g., `RelCountChangeCheck` now applies date-range and identifier filters when fetching data into memory). Added `__date_range__` handling in the executor and a new integration test covering the rolling-window fetch behavior.
- Support filters referencing nested JSON fields (e.g., `value.shopId`) when data is flattened in-memory by using the last dot-separated segment for WHERE and SELECT operations while preserving the original nested `COLUMN` value in results and persisted rows.

## [0.10.0] - 2026-01-20

### Added

- Allow setting `monitor_only` on check bundles and on individual checks
- Map provider/table-not-found errors to a unified `table_exists` metric
- Support identifier-type filters without explicit column/value for naming; add `identifier_format` option (`identifier`, `filter_name`, `column_name`) to control the result identifier column
- Treat identifier filters with missing/null `value` as a configurable placeholder for logging and naming (defaults to `ALL`)
- Add `identifier_placeholder` option to configure the placeholder value used when identifier filters lack a value; defaults to `ALL` and is applied to the result IDENTIFIER column and logging for clearer partition naming.

### Fixed

- Quote table identifiers in bulk SELECTs when loading data into DuckDB memory to avoid BigQuery binder errors for identifiers that look like project IDs (e.g., `EC0601`). Added an integration test covering the quoting behavior.
- Ensure MatchRateCheck only requires the check column from the left table; the right table now only contributes join and filter columns to avoid unnecessary column selection and errors.

## [0.9.0] - 2026-01-16

### Changed

- Refactor executor to bulk load data into DuckDB memory and run checks from there
- Update ValuesInSetCheck config to require value_set as a set of str, bool, or int
- Refactor IqrOutlierCheck to consistently exclude date filters from WHERE clauses and data requirements

### Fixed

- Fix data_exists query in MatchRate check and improve error handling for empty tables
- Improve filter handling in SQL assembly and enforce identifier filter presence in check validation

## [0.8.0] - 2026-01-15

### Fixed

- **IqrOutlierCheck**: Prevent `TypeError` when `IqrOutlierCheck` is initialized with unexpected `lower_threshold` or 
  `upper_threshold` arguments by filtering them in the executor.

## [0.7.0] - 2026-01-08

### Fixed

- Fix query wrapping
- Fix date column type casting in SQL queries to handle TIMESTAMP values correctly
- Fix format_threshold to cast infinity values in query

## [0.6.0] - 2026-01-07

### Added

- Add format_threshold utility to handle SQL threshold values and update executor to use it

### Changed

- Refactor threshold handling to use math.inf for infinite values and add tests for YAML parsing of infinite thresholds

## [0.5.0] - 2026-01-06

### Added

- Add progress bar for check execution
- **Data existence check caching**: Implement caching mechanism in `CheckExecutor` to eliminate redundant data existence queries when multiple checks operate on the same dataset (same table, date, and filters). The cache is automatically managed per executor instance and can significantly improve performance - e.g., 10 checks on the same dataset now execute only 1 data existence query instead of 10 (90% reduction)

### Changed

- Refactor database provider handling in executor.py
- Update dependency versions to enforce upper limits

## [0.4.1] - 2025-12-23

### Fixed

- **RegexMatchCheck model**: Fixed `check_type` literal in `_RegexMatchCheck` model from incorrect `"NullRatioCheck"` to `"RegexMatchCheck"`. This bug prevented YAML configurations using `check_type: RegexMatchCheck` from being parsed correctly.

## [0.4.0] - 2025-12-23

### Added

- **Database setup variables**: New `--database_setup_variable` (`-dsv`) option for `run` and `print` commands to substitute `${VAR_NAME}` placeholders in `database_setup` SQL. This enables dynamic configuration of database connections (e.g., different GCP projects per environment). Variables can also be provided via the `DATABASE_SETUP_VARIABLES` environment variable using comma-separated `VAR=value` pairs. CLI options override environment variables.

## [0.3.0] - 2025-12-23

### Added

- **CLI overwrites**: New `--overwrites` (`-o`) option for `run` and `print` commands to override configuration values at runtime. Supports flexible path-based syntax:
  - Filter values: `-o partition_date=2023-01-01` or `-o filters.partition_date=2023-01-01`
  - Filter fields: `-o filters.partition_date.column=OTHER_COL` or `-o filters.amount.operator=>=`
  - Other defaults: `-o identifier_format=column_name` or `-o monitor_only=false`
  - Bundle-level: `-o check_bundles.my_bundle.filters.partition_date=2023-01-01`
  - Check-level: `-o check_bundles.my_bundle.0.table=other_table`

### Changed

- **BREAKING**: Removed `offset` property from filter configuration. Date offsets are now specified inline in the value string (e.g., `yesterday-2`, `today+1`, `tomorrow-3`). This simplifies configuration and makes the offset more visible at the point of use.

## [0.2.0] - 2025-12-23

### Added

- AverageCheck to compute AVG over a column.
- MaxCheck to compute MAX over a column.
- MinCheck to compute MIN over a column.
- Lint (almost) all rules
- CLI subcommands: `run`, `validate`, and `print`
  - `run`: Execute checks (previously the default behavior)
  - `validate`: Validate configuration without execution
  - `print`: Print resolved configuration in model/yaml/json formats with configurable indentation
- **Filters in global defaults**: Filters can now be configured in global `defaults` and are merged with bundle-level and check-level filters
- **Identifier filter type**: New filter type `identifier` to mark filters that identify data partitions (e.g., shop, tenant)
- **Identifier format configuration**: New `identifier_format` global setting with three options:
  - `identifier` (default): Result column `IDENTIFIER` with value `column=value`
  - `filter_name`: Result column uses filter name (e.g., `SHOP_ID`), value only
  - `column_name`: Result column uses database column name (e.g., `SHOP_CODE`), value only
- **NULL value filtering**: Filters now support `null` values for `IS NULL` / `IS NOT NULL` filtering
- **Validation for identifier consistency**: When using `filter_name` or `column_name` format, all identifier filters must have the same filter name or column name respectively

### Changed

- Replace `creosote` with `deptry` for unused dependency checking
- Replace `pip-audit` with `uv-secure` for vulnerability checking
- Add missing type hints and migrate Optional/Union to modern union syntax
- **BREAKING**: CLI now uses subcommands. Previous `koality --config_path <file>` is now `koality run --config_path <file>`
- **BREAKING**: Check class constructors now use explicit parameters instead of `**kwargs` for `filters`, `identifier_format`, `date_info`, `extra_info`, and `monitor_only`
- **BREAKING**: Reordered keyword-only parameters in check classes: `filters`, `identifier_format`, `date_info`, `extra_info`, `monitor_only` (monitor_only is now last)

### Removed

- Remove dynamic CLI argument parsing (`parse_arg` utility and extra args support in `run` command)

## [0.1.0] - 2025-12-09

### Added

- Initial release of Koality - a Python library for data quality monitoring using DuckDB
- Core check classes for data validation:
  - `NullRatioCheck` - validate null ratios in columns
  - `RegexMatchCheck` - validate values against regex patterns
  - `ValuesInSetCheck` - validate values are within allowed sets
  - `RollingValuesInSetCheck` - rolling window variant of values in set check
  - `DuplicateCheck` - detect duplicate records
  - `CountCheck` - validate row counts
  - `OccurrenceCheck` - validate occurrence counts
  - `IqrOutlierCheck` - detect outliers using interquartile range
  - `MatchRateCheck` - validate match rates between two tables
  - `RelCountChangeCheck` - validate relative count changes over rolling windows
- `CheckExecutor` for orchestrating check execution with YAML configuration
- Pydantic models for configuration validation with default propagation
- CLI interface via `koality --config_path <file>`
- Support for attaching external databases (BigQuery, Postgres, etc.) through DuckDB
- Dynamic filter system for conditional check execution
- Result persistence to database tables
- Comprehensive documentation with MkDocs
- Unit and integration test suites
- GitHub Actions workflows for CI/CD and releases
- Use common `defaults` key instead of `global_defaults` and `default_args`

[Unreleased]: https://github.com/ottogroup/koality/compare/v0.11.0...HEAD
[0.11.0]: https://github.com/ottogroup/koality/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/ottogroup/koality/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/ottogroup/koality/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/ottogroup/koality/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/ottogroup/koality/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/ottogroup/koality/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/ottogroup/koality/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/ottogroup/koality/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/ottogroup/koality/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ottogroup/koality/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ottogroup/koality/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ottogroup/koality/releases/tag/v0.1.0