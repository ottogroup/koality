# Configuration

Koality uses YAML configuration files to define data quality checks. This page describes the configuration schema and available options.

## Configuration Schema

```yaml
name: string                    # Name of the configuration
database_setup: string          # SQL to set up database connections
database_accessor: string       # Database accessor prefix for tables

defaults:                # Global default settings
  monitor_only: bool            # If true, checks don't fail the run
  result_table: string          # Table to store results (optional)
  log_path: string              # Path to write failed checks log (optional)
  date_filter_column: string    # Default date filter column
  date_filter_value: string     # Default date filter value
  filter_column: string         # Default filter column
  filter_value: string          # Default filter value

check_bundles:                  # List of check bundles
  - name: string                # Bundle name
    defaults:               # Default arguments for checks in this bundle
      table: string
      check_column: string
      # ... other defaults
    checks:                     # List of checks
      - check_type: string      # Type of check (required)
        # ... check-specific arguments
```

## Global Defaults

Global defaults are applied to all checks unless overridden at the bundle or check level.

| Field                | Type   | Description                                          |
|----------------------|--------|------------------------------------------------------|
| `monitor_only`       | bool   | If `true`, check failures don't fail the overall run |
| `result_table`       | string | Table name for persisting check results              |
| `log_path`           | string | File path to write failed checks log                 |
| `date_filter_column` | string | Column name for date filtering                       |
| `date_filter_value`  | string | Value for date filtering                             |
| `filter_column`      | string | Column name for generic filtering                    |
| `filter_value`       | string | Value for generic filtering                          |

## Check Bundles

Check bundles group related checks together and can define shared default arguments.

```yaml
check_bundles:
  - name: orders_quality
    defaults:
      table: orders
      date_filter_column: order_date
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

Rolling window version of ValuesInSetCheck (14-day window).

```yaml
- check_type: RollingValuesInSetCheck
  table: my_table
  check_column: category
  value_set: ["A", "B", "C"]
  date_filter_column: created_at
  date_filter_value: "2024-01-01"
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

Detects relative count changes over a rolling window.

```yaml
- check_type: RelCountChangeCheck
  table: my_table
  check_column: "*"
  rolling_days: 7
  date_filter_column: date
  date_filter_value: "2024-01-01"
  lower_threshold: 0.8    # At least 80% of average
  upper_threshold: 1.2    # At most 120% of average
```

### IqrOutlierCheck

Detects outliers using interquartile range.

```yaml
- check_type: IqrOutlierCheck
  table: my_table
  check_column: amount
  date_filter_column: date
  date_filter_value: "2024-01-01"
  interval_days: 30
  how: "both"             # "both", "upper", or "lower"
  iqr_factor: 1.5
  lower_threshold: 0.0
  upper_threshold: 0.05   # Max 5% outliers
```

## Filtering

Checks support filtering using column/value pairs:

```yaml
- check_type: CountCheck
  table: orders
  check_column: "*"
  shop_id_filter_column: shop_code
  shop_id_filter_value: "SHOP01"
  date_filter_column: order_date
  date_filter_value: "2024-01-01"
```

The filter pattern is `{name}_filter_column` paired with `{name}_filter_value`.

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
  date_filter_column: date
  date_filter_value: "2024-01-01"

check_bundles:
  - name: orders
    defaults:
      table: orders
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