# MatchRateCheck — check_column placement

Guidance:

- The `check_column` for `MatchRateCheck` must be a column present in the left-hand table only.
- The right-hand table is only expected to provide join columns and filter columns; it must not be required to contain `check_column`.

Configuration example:

```yaml
- defaults:
    check_type: MatchRateCheck
    check_column: product_number                   # must exist on the left table
    join_columns_left:
      - BQ_PARTITIONTIME
      - shopId
      - product_number
    join_columns_right:
      - BQ_PARTITIONTIME
      - value.shopId
      - product_number
  checks:
    - left_table: project.dataset.left_table
      right_table: project.dataset.right_table
      filters:
        shop_id:
          value: SHOP01
```

Notes:

- The executor only requests the `check_column` from the left table during bulk loading; the right table will only be queried for its join and filter columns.
- This avoids errors when the right table does not contain the `check_column` or when its identifier resembles a BigQuery project ID.
