Identifier Placeholder

When an identifier-type filter (type: identifier) is defined but the filter's value is missing or explicitly `null`, Koality uses a configurable placeholder string to fill the result IDENTIFIER column and logging messages. This avoids `None` or empty identifiers in persisted results and monitoring UIs.

Configuration

You can set the placeholder in the following locations (more specific levels override less specific ones):

- `defaults.identifier_placeholder` (global default)
- `check_bundles.<bundle>.defaults.identifier_placeholder` (bundle-level)
- `check_bundles.<bundle>.checks.<i>.identifier_placeholder` (check-level)

If not set, the placeholder defaults to `ALL`.

Examples

1) Global default placeholder:

```yaml
defaults:
  identifier_placeholder: UNKNOWN
  filters:
    shop_id:
      column: shop_code
      type: identifier
      value: null
```

Result: IDENTIFIER uses `UNKNOWN` for checks that rely on the `shop_id` identifier when no concrete value is provided.

2) Bundle-level override:

```yaml
check_bundles:
  - name: my_bundle
    defaults:
      identifier_placeholder: ALL_SHOPS
      filters:
        shop_id:
          column: shop_code
          type: identifier
          value: null
```

3) Check-level override:

```yaml
check_bundles:
  - name: my_bundle
    defaults:
      filters:
        shop_id:
          column: shop_code
          type: identifier
    checks:
      - check_type: CountCheck
        identifier_placeholder: SHOP_UNKNOWN
        filters:
          shop_id:
            value: null
```

Notes

- The placeholder is only applied for naming/logging and does not produce a WHERE clause when the identifier filter lacks a `value` and no column is provided.
- Use a descriptive placeholder (e.g., `ALL`, `UNKNOWN`, `ALL_SHOPS`) to make results easier to interpret in dashboards and logs.
