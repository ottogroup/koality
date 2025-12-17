# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- AverageCheck to compute AVG over a column.
- MaxCheck to compute MAX over a column.
- MinCheck to compute MIN over a column.
- Lint (almost) all rules

### Changed

- Add missing type hints and migrate Optional/Union to modern union syntax

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

[Unreleased]: https://github.com/ottogroup/koality/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ottogroup/koality/releases/tag/v0.1.0