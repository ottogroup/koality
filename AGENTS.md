# GitHub Copilot Repository Instructions

Purpose: Guide Copilot to produce accurate, minimal, and maintainable changes for this repository.

## Project context
- Name: Koality – Data quality monitoring (DQM) library using DuckDB
- Language: Python
- Key domains: data quality checks (null ratios, regex matches, values-in-set, duplicates, counts, occurrence, IQR outliers, match-rate between tables, rolling count change), YAML config, DuckDB SQL
- CLI: Click-based command `koality --config_path <file>`

## Architecture (what matters for Copilot)
- src/koality/checks.py
  - Base classes: DataQualityCheck → ColumnTransformationCheck
  - Concrete checks: NullRatioCheck, RegexMatchCheck, ValuesInSetCheck (+ RollingValuesInSetCheck), DuplicateCheck, CountCheck, OccurrenceCheck, IqrOutlierCheck, MatchRateCheck, RelCountChangeCheck
  - Each check assembles SQL, runs via DuckDB, then compares against thresholds
- src/koality/executor.py
  - CheckExecutor loads YAML config, instantiates checks from CHECK_MAP, runs them, aggregates results, can persist results
- src/koality/models.py
  - Pydantic models validate YAML; defaults propagate from `defaults` → `check_bundles.defaults` → individual checks
- src/koality/cli.py
  - Click entrypoint invoking the executor
- src/koality/utils.py
  - `identify_database_provider()` inspects attached DBs to adapt SQL; filters built via `assemble_where_statement()` from `{name}_filter_*` pairs

## Setup and developer commands
- Dependencies: `uv sync --all-groups`
- Tests: `poe test` (or `poe test_unit`, `poe test_integration`)
- Lint/format: `poe lint` (check), `poe format` (auto-fix)
- Docs: `poe docs_serve`
- Security: `poe check_vulnerable_dependencies`, `poe check_unused_dependencies`

## Testing guidance
- Tests live in `tests/unit` and `tests/integration` with pytest markers
- Integration tests use in-memory DuckDB fixtures from `tests/conftest.py`
- When adding features, include focused unit tests and, when SQL/duckdb behavior is involved, an integration test

## Coding style and constraints
- Make the smallest possible change; do not refactor unrelated code
- Prefer using existing utilities (e.g., filter assembly, provider detection) over new helpers
- Avoid introducing new dependencies unless strictly necessary
- Follow existing patterns for new checks: subclass appropriate base, implement SQL assembly method(s), compare to thresholds, ensure serialization/aggregation compatibility
- Keep comments sparse and only where non-obvious

## Configuration model expectations
- YAML → Pydantic `Config` with default propagation
- Filter system: checks accept `{name}_filter_column` + `{name}_filter_value` parameter pairs; use `get_filters()` and `assemble_where_statement()`
- DB provider detection informs SQL differences; avoid vendor-specific SQL unless guarded by provider checks

## CLI behavior
- Primary command: `koality --config_path <file>`
- Executor coordinates loading config, running checks, aggregating results, and optional persistence

## Security and privacy
- Never hardcode or commit secrets
- Keep examples and tests self-contained (in-memory DuckDB); do not call external services

## PR expectations
- Add/adjust only the code required for the task
- Update or add tests directly related to the change; keep existing behavior intact
- Run `poe lint` and `poe test` locally; ensure both pass

## How Copilot should respond when editing this repo
- Be concise; propose surgical diffs touching the minimum lines/files
- Reuse repo commands and architecture; don’t invent new workflows
- For SQL, prefer portable DuckDB-compatible syntax unless gated by provider checks
- If adding a new check, document it briefly in code and add minimal tests validating SQL and threshold behavior
