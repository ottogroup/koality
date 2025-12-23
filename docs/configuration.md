# Configuration

Koality uses YAML configuration files to define data quality checks. This page describes the configuration schema and available options.

## Supported Databases

Koality uses DuckDB as its query engine. Currently supported databases:

| Database              | Status            |
|-----------------------|-------------------|
| DuckDB (in-memory)    | ✅ Fully supported |
| Google Cloud BigQuery | ✅ Fully supported |

External databases are accessed through DuckDB extensions. For BigQuery, queries are executed using the BigQuery Jobs API for read operations and `bigquery_execute` for write operations.
Other databases may need custom handling in [`execute_query`](machinery/utils.md)!

## Configuration Schema

```yaml
name: string                    # Name of the configuration
database_setup: string          # SQL to set up database connections
database_accessor: string       # Database accessor prefix for tables

defaults:                       # Global default settings
  monitor_only: bool            # If true, checks don't fail the run
  result_table: string          # Table to store results (optional)
  log_path: string              # File path to write failed checks log (optional)
  identifier_format: string     # How to format identifier in results (optional)
  filters:                      # Default filters applied to all checks
    <filter_name>:
      column: string            # Column to filter on
      value: any                # Filter value (use null for IS NULL)
      type: date | identifier | other  # Filter type
      operator: string          # Comparison operator (default: "=")
      parse_as_date: bool       # Parse value as date (default: false)

check_bundles:                  # List of check bundles
  - name: string                # Bundle name
    defaults:                   # Default arguments for checks in this bundle
      table: string
      check_column: string
      filters: {...}            # Bundle-level default filters
      # ... other defaults
    checks:                     # List of checks
      - check_type: string      # Type of check (required)
        filters: {...}          # Check-level filters (merged with defaults)
        # ... check-specific arguments
```

## Database Connection

The `database_setup` and `database_accessor` fields work together to configure how Koality connects to your data:

### database_setup

A SQL string that is executed when the `CheckExecutor` is initialized (if no custom DuckDB client is provided). Use this to:

- Install and load DuckDB extensions
- Attach external databases
- Configure connection settings

### database_accessor

The name/alias of the attached database. This is used to:

- Identify the database provider type (e.g., "bigquery")
- Prefix table references in queries
- Route queries through the appropriate execution method

### Examples

**DuckDB (in-memory with local file):**

```yaml
name: local_checks
database_setup: |
  ATTACH 'warehouse.duckdb' AS warehouse;
database_accessor: warehouse
```

**Google Cloud BigQuery:**

```yaml
name: bigquery_checks
database_setup: |
  INSTALL bigquery;
  LOAD bigquery;
  ATTACH 'project=my-gcp-project' AS bq (TYPE bigquery, READ_ONLY);
database_accessor: bq
```

When using BigQuery, Koality automatically:

- Uses `bigquery_query()` for SELECT operations (supports views)
- Uses `bigquery_execute()` for write operations (INSERT, CREATE, etc.)
- Maps DuckDB types to BigQuery types (e.g., VARCHAR → STRING, NUMERIC → FLOAT64)

### Database Setup Variables

The `database_setup` field supports dynamic variable substitution using `${VAR_NAME}` syntax. This is useful for:

- Using different GCP projects per environment (dev/staging/prod)
- Keeping sensitive configuration out of YAML files
- Enabling reusable configurations across different contexts

**Using CLI option (`-dsv`):**

```bash
# Single variable
koality run --config_path checks.yaml -dsv PROJECT_ID=my-gcp-project

# Multiple variables
koality run --config_path checks.yaml -dsv PROJECT_ID=prod-project -dsv DATASET=analytics
```

**Using environment variable:**

```bash
# Comma-separated VAR=value pairs
DATABASE_SETUP_VARIABLES="PROJECT_ID=my-project,DATASET=prod" koality run --config_path checks.yaml
```

**Example configuration:**

```yaml
name: bigquery_checks
database_setup: |
  INSTALL bigquery;
  LOAD bigquery;
  ATTACH 'project=${PROJECT_ID}' AS bq (TYPE bigquery, READ_ONLY);
database_accessor: bq
```

**Variable resolution order:**

1. CLI options (`-dsv`) take highest priority
2. Environment variable (`DATABASE_SETUP_VARIABLES`) is used as fallback
3. If a variable is referenced but not provided, an error is shown with a helpful hint

**Preview with print:**

Use the `print` command to verify variable substitution before running checks:

```bash
koality print --config_path checks.yaml -dsv PROJECT_ID=my-project --format yaml
```

## Global Defaults

Global defaults are applied to all checks unless overridden at the bundle or check level.

| Field               | Type   | Description                                                   |
|---------------------|--------|---------------------------------------------------------------|
| `monitor_only`      | bool   | If `true`, check failures don't fail the overall run          |
| `result_table`      | string | Table name for persisting check results                       |
| `log_path`          | string | File path to write failed checks log                          |
| `identifier_format` | string | How to format identifier in output (see below)                |
| `filters`           | object | Default filters applied to all checks (see Filtering section) |

### Identifier Format

The `identifier_format` option controls how the identifier filter value appears in check results and messages. Three options are available:

| Format        | Result Column | Value Format   | Example                               |
|---------------|---------------|----------------|---------------------------------------|
| `identifier`  | `IDENTIFIER`  | `column=value` | `shop_code=SHOP001`                   |
| `filter_name` | Filter name   | Value only     | Column: `SHOP_ID`, Value: `SHOP001`   |
| `column_name` | Column name   | Value only     | Column: `SHOP_CODE`, Value: `SHOP001` |

**Default**: `identifier`

**Consistency requirement**: When using `filter_name` or `column_name` format, all identifier filters across all checks must use the same filter name or column name respectively. This ensures consistent column headers in results. The `identifier` format has no such restriction since it always uses `IDENTIFIER` as the column header.

```yaml
defaults:
  identifier_format: identifier  # or "filter_name" or "column_name"
  filters:
    shop_id:
      column: shop_code
      value: SHOP001
      type: identifier
```

## Check Bundles

Check bundles group related checks together and can define shared default arguments.

```yaml
check_bundles:
  - name: orders_quality
    defaults:
      table: orders
      filters:
        partition_date:
          column: order_date
          value: yesterday
          type: date
    checks:
      - check_type: NullRatioCheck
        check_column: customer_id
        upper_threshold: 0.01
```

## Check Types

### NullRatioCheck

Validates the ratio of null values in a column.

```yaml
- check_type: NullRatioCheck
  table: my_table
  check_column: my_column
  lower_threshold: 0.0    # Minimum allowed null ratio
  upper_threshold: 0.05   # Maximum allowed null ratio
```

### RegexMatchCheck

Checks if column values match a regex pattern.

```yaml
- check_type: RegexMatchCheck
  table: my_table
  check_column: email
  regex_to_match: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
  lower_threshold: 0.95
  upper_threshold: 1.0
```

### ValuesInSetCheck

Validates that column values are within an allowed set.

```yaml
- check_type: ValuesInSetCheck
  table: my_table
  check_column: status
  value_set: ["pending", "active", "completed"]
  lower_threshold: 1.0
  upper_threshold: 1.0
```

### RollingValuesInSetCheck

Rolling window version of ValuesInSetCheck (14-day window). Requires a filter with `type: date`.

```yaml
- check_type: RollingValuesInSetCheck
  table: my_table
  check_column: category
  value_set: ["A", "B", "C"]
  filters:
    partition_date:
      column: created_at
      value: yesterday
      type: date
  lower_threshold: 0.95
```

### DuplicateCheck

Detects duplicate values in a column.

```yaml
- check_type: DuplicateCheck
  table: my_table
  check_column: unique_id
  lower_threshold: 0.0
  upper_threshold: 0.0    # 0 means no duplicates allowed
```

### CountCheck

Validates row counts or distinct counts.

```yaml
- check_type: CountCheck
  table: my_table
  check_column: "*"       # Use "*" for row count
  distinct: false         # Set to true for distinct count
  lower_threshold: 1000
  upper_threshold: 100000
```

### OccurrenceCheck

Checks minimum or maximum occurrence of any value.

```yaml
- check_type: OccurrenceCheck
  table: my_table
  check_column: product_id
  max_or_min: "max"       # "max" or "min"
  upper_threshold: 100    # No value should occur more than 100 times
```

### MatchRateCheck

Validates join match rates between two tables.

```yaml
- check_type: MatchRateCheck
  left_table: orders
  right_table: products
  check_column: product_id
  join_columns: ["product_id"]
  # Or use different column names:
  # join_columns_left: ["prod_id"]
  # join_columns_right: ["product_id"]
  lower_threshold: 0.95
```

### RelCountChangeCheck

Detects relative count changes over a rolling window. Requires a filter with `type: date`.

```yaml
- check_type: RelCountChangeCheck
  table: my_table
  check_column: "*"
  rolling_days: 7
  filters:
    partition_date:
      column: date
      value: yesterday
      type: date
  lower_threshold: -0.2   # At least 80% of average
  upper_threshold: 0.2    # At most 120% of average
```

### IqrOutlierCheck

Detects outliers using interquartile range. Requires a filter with `type: date`.

```yaml
- check_type: IqrOutlierCheck
  table: my_table
  check_column: amount
  filters:
    partition_date:
      column: date
      value: yesterday
      type: date
  interval_days: 30
  how: "both"             # "both", "upper", or "lower"
  iqr_factor: 1.5
  lower_threshold: 0.0
  upper_threshold: 0.05   # Max 5% outliers
```

## Filtering

Checks support filtering using a structured `filters` configuration:

```yaml
- check_type: CountCheck
  table: orders
  check_column: "*"
  filters:
    partition_date:
      column: order_date
      value: yesterday
      type: date
    shop_id:
      column: shop_code
      value: "SHOP01"
      type: identifier
```

### Filter Configuration

Each filter has the following properties:

| Property        | Type   | Required | Description                                                                                                                            |
|-----------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------------------|
| `column`        | string | Yes*     | The database column name to filter on                                                                                                  |
| `value`         | any    | Yes*     | The filter value (string, number, list for IN operators, or `null` for IS NULL). For dates, supports inline offsets like `yesterday-2` |
| `operator`      | string | No       | SQL operator: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `LIKE`. Default: `=`                                                    |
| `type`          | string | No       | Filter type: `date`, `identifier`, or `other`. Default: `other`                                                                        |
| `parse_as_date` | bool   | No       | If `true`, parse the value as a date (supports relative dates). Default: `false`                                                       |

*`column` and `value` are optional in defaults (global or bundle level) but must be set after merging. This allows defining partial filters in defaults that are completed at the check level.

### Filter Types

Koality supports three filter types:

| Type         | Purpose                                      | Limit         |
|--------------|----------------------------------------------|---------------|
| `date`       | Date filter for rolling checks               | One per check |
| `identifier` | Identifier for grouping results (e.g., shop) | One per check |
| `other`      | Regular filters                              | Unlimited     |

### Identifier Filters

Use `type: identifier` to mark the filter that identifies your data partition (e.g., shop, tenant, region):

```yaml
filters:
  shop_id:
    column: shop_code
    value: SHOP001
    type: identifier
```

The identifier value appears in check results and failure messages. How it's formatted depends on the `identifier_format` global setting.

### Date Filters

When `type: date` is set, the value is automatically parsed as a date. Supported formats:

- **ISO dates**: `2024-01-15`, `20240115`
- **Relative dates**: `today`, `yesterday`, `tomorrow`
- **With inline offset**: `today-2`, `yesterday+1`, `tomorrow-3` (add or subtract days directly in the value)

```yaml
filters:
  partition_date:
    column: BQ_PARTITIONTIME
    value: yesterday-1    # 2 days ago (yesterday minus 1 day)
    type: date
```

**Important**: Only one filter with `type: date` is allowed per check. This is the filter used by rolling checks (`RollingValuesInSetCheck`, `RelCountChangeCheck`, `IqrOutlierCheck`) for their date-based calculations.

If you need to parse multiple date values, use `parse_as_date: true` for additional filters:

```yaml
filters:
  partition_date:
    column: BQ_PARTITIONTIME
    value: yesterday
    type: date              # THE date filter for rolling checks
  created_after:
    column: created_at
    value: today-7          # 7 days ago
    parse_as_date: true     # Also parses date, but isn't "the" date filter
    operator: ">="
```

### Operators

Use the `operator` property for different comparison types:

```yaml
filters:
  # Equality (default)
  status:
    column: order_status
    value: completed

  # Greater than
  revenue:
    column: total_revenue
    value: 1000
    operator: ">="

  # IN operator (list of values)
  category:
    column: category
    value: ["toys", "electronics", "clothing"]
    operator: "IN"

  # NOT IN operator
  excluded:
    column: region
    value: ["test", "staging"]
    operator: "NOT IN"

  # LIKE operator (pattern matching)
  email_domain:
    column: email
    value: "%@example.com"
    operator: "LIKE"

  # IS NULL (filter for missing values)
  not_deleted:
    column: deleted_at
    value: null           # or ~ or empty
    operator: "="         # generates: deleted_at IS NULL

  # IS NOT NULL (filter for existing values)
  has_email:
    column: email
    value: null
    operator: "!="        # generates: email IS NOT NULL
```

### Default Filters

Filters can be defined at any level (global defaults, bundle defaults, or individual checks) and are merged with inheritance:

```yaml
defaults:
  filters:
    partition_date:
      column: DATE
      value: yesterday
      type: date

check_bundles:
  - name: orders
    defaults:
      filters:
        shop_id:
          column: shop_code
          value: "SHOP01"
    checks:
      - check_type: CountCheck
        table: orders
        check_column: "*"
        # Inherits both partition_date and shop_id filters
```

### Partial Filters in Defaults

You can define partial filters in defaults (omitting `column` or `value`) and complete them at a lower level:

```yaml
defaults:
  filters:
    shop_id:
      column: shopId       # Define column once
      type: identifier     # Define type once
      # value not set - must be set per check

check_bundles:
  - name: shop1_checks
    defaults:
      table: orders
      filters:
        shop_id:
          value: SHOP001   # Set value, inherits column and type
    checks:
      - check_type: CountCheck
        check_column: "*"

  - name: shop2_checks
    defaults:
      table: orders
    checks:
      - check_type: CountCheck
        check_column: "*"
        filters:
          shop_id:
            value: SHOP002   # Set value at check level
```

## Example Configuration

```yaml
name: ecommerce_dqm
database_setup: |
  ATTACH 'warehouse.duckdb' AS warehouse;
database_accessor: warehouse

defaults:
  monitor_only: false
  result_table: dqm_results
  log_path: /var/log/dqm/failed_checks.log
  identifier_format: identifier  # Results show "shop_code=SHOP01"
  filters:
    partition_date:
      column: date
      value: yesterday
      type: date

check_bundles:
  - name: orders
    defaults:
      table: orders
      filters:
        shop_id:
          column: shop_code
          value: "SHOP01"
          type: identifier
    checks:
      - check_type: NullRatioCheck
        check_column: order_id
        upper_threshold: 0.0

      - check_type: CountCheck
        check_column: "*"
        lower_threshold: 100

  - name: products
    defaults:
      table: products
    checks:
      - check_type: DuplicateCheck
        check_column: sku
        upper_threshold: 0.0

      - check_type: ValuesInSetCheck
        check_column: status
        value_set: ["active", "inactive", "discontinued"]
        lower_threshold: 1.0
```

## CLI Overwrites

The `--overwrites` (`-o`) option allows overriding configuration values at runtime without modifying the YAML file. This is useful for:

- Running checks for a specific date instead of "yesterday"
- Testing with different filter values
- Temporarily changing settings like `monitor_only`

### Overwrite Syntax

Overwrites use a dot-notation path to target specific values, mirroring the YAML structure:

```bash
# Override filter value (targets the "value" field by default)
koality run --config_path checks.yaml -o defaults.filters.partition_date=2023-06-15

# Override filter field (column, operator, type, etc.)
koality run --config_path checks.yaml -o defaults.filters.partition_date.column=OTHER_DATE_COL
koality run --config_path checks.yaml -o defaults.filters.amount.operator=">="

# Override other defaults
koality run --config_path checks.yaml -o defaults.identifier_format=column_name
koality run --config_path checks.yaml -o defaults.monitor_only=true

# Multiple overwrites
koality run --config_path checks.yaml -o defaults.filters.partition_date=2023-06-15 -o defaults.filters.shop_id=SHOP02
```

### Overwrite Levels

Overwrites can target different levels of the configuration hierarchy:

```bash
# Global defaults (propagates to all checks)
-o defaults.filters.partition_date=2023-06-15
-o defaults.identifier_format=column_name

# Bundle-level defaults (only affects that bundle's checks)
-o check_bundles.orders.filters.partition_date=2023-06-15
-o check_bundles.orders.identifier_format=filter_name

# Check-level (only affects a specific check by index)
-o check_bundles.orders.0.table=orders_archive
-o check_bundles.orders.0.filters.partition_date=2023-06-15
```

### Propagation

Overwrites applied to global defaults automatically propagate to all checks through the normal default inheritance mechanism. This means:

```bash
# This single overwrite...
koality run --config_path checks.yaml -o partition_date=2023-06-15

# ...affects ALL checks that inherit the partition_date filter from defaults
```

### Preview with Print

Use the `print` command with overwrites to verify your overrides before running checks:

```bash
# Preview the resolved configuration with overwrites applied
koality print --config_path checks.yaml -o partition_date=2023-06-15 --format yaml
```