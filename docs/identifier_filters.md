Identifier filters and naming

Koality supports an `identifier` filter type to mark the field that identifies data partitions, for example a shop or tenant. This page documents how to configure the identifier and how it appears in results.

identifier_format

Set the global `identifier_format` in the `defaults` section of your YAML config. Options:

- `identifier` (default): result rows contain an `IDENTIFIER` column with the value formatted as `column=value` (e.g., `shop_code=EC0601`).
- `filter_name`: result rows use the filter name as the column header (e.g., `SHOP_ID`) and the cell contains the identifier value only.
- `column_name`: result rows use the database column name as the column header (e.g., `SHOP_CODE`) and the cell contains the identifier value only.

Naming-only identifier filters

You may define an identifier-type filter in global `defaults` without specifying a concrete `column` or `value` when you only want to control the result column name. Example:

```yaml
defaults:
  identifier_format: filter_name
  filters:
    shop_id:
      type: identifier
```

In this example, `shop_id` acts as a hint: Koality will use `SHOP_ID` as the result column name but will not add a WHERE clause for it. Checks can still provide concrete `column`/`value` pairs at the bundle or check level when filtering by an identifier is required.

Examples

1) Use filter name as result column name, and set values per-check:

```yaml
defaults:
  identifier_format: filter_name
  filters:
    shop_id:
      type: identifier

check_bundles:
  - name: sales_checks
    defaults:
      table: analytics.sales
    checks:
      - filters:
          shop_id:
            column: shop_code
            value: EC0601
      - filters:
          shop_id:
            column: shop_code
            value: EC0602
```

2) Use column name as result column and supply value in defaults:

```yaml
defaults:
  identifier_format: column_name
  filters:
    shop_id:
      column: shop_code
      value: EC0601
      type: identifier
```

Notes

- When `identifier_format` is `filter_name` or `column_name`, Koality validates that identifier filters across checks are consistent (same filter name or same column name respectively).
- Identifier-type filters without a `column` or `value` are ignored when assembling WHERE clauses; they only affect naming.
