"""Unit tests for filter parsing functionality."""

import datetime as dt

import pytest
from pydantic_yaml import parse_yaml_raw_as

from koality.checks import DataQualityCheck
from koality.models import Config, FilterConfig, _Defaults

pytestmark = pytest.mark.unit


class TestNewFiltersSyntax:
    """Tests for the new structured filters syntax."""

    def test_simple_filter(self) -> None:
        """Test parsing a simple filter with column and value."""
        kwargs = {
            "filters": {
                "shop_id": {"column": "shop_code", "value": "SHOP001"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "=", "type": "other"}}

    def test_multiple_filters(self) -> None:
        """Test parsing multiple filters."""
        kwargs = {
            "filters": {
                "shop_id": {"column": "shop_code", "value": "SHOP001"},
                "region": {"column": "region_code", "value": "EU"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {
            "shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "=", "type": "other"},
            "region": {"column": "region_code", "value": "EU", "operator": "=", "type": "other"},
        }

    def test_date_type_with_iso_date(self) -> None:
        """Test type='date' with an ISO date string (no parsing needed)."""
        kwargs = {
            "filters": {
                "date": {
                    "column": "DATE",
                    "value": "2023-01-15",
                    "type": "date",
                },
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"date": {"column": "DATE", "value": "2023-01-15", "operator": "=", "type": "date"}}

    def test_date_type_with_yesterday(self) -> None:
        """Test type='date' auto-parses 'yesterday' relative date."""
        kwargs = {
            "filters": {
                "date": {
                    "column": "DATE",
                    "value": "yesterday",
                    "type": "date",
                },
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()
        assert result == {"date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"}}

    def test_date_type_with_today(self) -> None:
        """Test type='date' auto-parses 'today' relative date."""
        kwargs = {
            "filters": {
                "date": {
                    "column": "DATE",
                    "value": "today",
                    "type": "date",
                },
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = dt.datetime.now(tz=dt.UTC).date().isoformat()
        assert result == {"date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"}}

    def test_date_type_with_inline_offset(self) -> None:
        """Test type='date' with inline offset in value."""
        kwargs = {
            "filters": {
                "date": {
                    "column": "DATE",
                    "value": "today-2",
                    "type": "date",
                },
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=2)).isoformat()
        assert result == {"date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"}}

    def test_date_type_with_yesterday_offset(self) -> None:
        """Test type='date' with yesterday and inline offset."""
        kwargs = {
            "filters": {
                "date": {
                    "column": "DATE",
                    "value": "yesterday-2",
                    "type": "date",
                },
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=3)).isoformat()
        assert result == {"date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"}}

    def test_date_type_auto_parses(self) -> None:
        """Test that type='date' automatically parses relative date values."""
        kwargs = {
            "filters": {
                "date": {
                    "column": "DATE",
                    "value": "yesterday",
                    "type": "date",
                },
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()
        assert result == {"date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"}}

    def test_other_type_does_not_parse(self) -> None:
        """Test that type='other' does not parse date-like values."""
        kwargs = {
            "filters": {
                "status": {
                    "column": "STATUS",
                    "value": "yesterday",  # This should NOT be parsed
                    "type": "other",
                },
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        # Value should remain as-is since type is "other"
        assert result == {"status": {"column": "STATUS", "value": "yesterday", "operator": "=", "type": "other"}}

    def test_filter_with_filter_config_object(self) -> None:
        """Test parsing using FilterConfig Pydantic model."""
        kwargs = {
            "filters": {
                "shop_id": FilterConfig(column="shop_code", value="SHOP001"),
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "=", "type": "other"}}

    def test_filter_with_filter_config_and_date_type(self) -> None:
        """Test FilterConfig with type='date' auto-parses value."""
        kwargs = {
            "filters": {
                "date": FilterConfig(
                    column="DATE",
                    value="yesterday",
                    type="date",
                ),
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()
        assert result == {"date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"}}

    def test_filter_with_filter_config_and_inline_offset(self) -> None:
        """Test FilterConfig with inline offset in value."""
        kwargs = {
            "filters": {
                "date": FilterConfig(
                    column="DATE",
                    value="yesterday-1",
                    type="date",
                ),
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=2)).isoformat()
        assert result == {"date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"}}

    def test_filter_without_column_is_skipped(self) -> None:
        """Test that filters without a column are skipped."""
        kwargs = {
            "filters": {
                "shop_id": {"value": "SHOP001"},  # No column
                "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"date": {"column": "DATE", "value": "2023-01-01", "operator": "=", "type": "date"}}

    def test_empty_filters(self) -> None:
        """Test with empty filters dict."""
        kwargs = {"filters": {}}
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {}

    def test_mixed_date_and_regular_filters(self) -> None:
        """Test combination of date filter with parse_as_date and regular filters."""
        kwargs = {
            "filters": {
                "date": {
                    "column": "DATE",
                    "value": "yesterday",
                    "type": "date",
                },
                "shop_id": {"column": "shop_code", "value": "SHOP001"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        expected_date = (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()
        assert result == {
            "date": {"column": "DATE", "value": expected_date, "operator": "=", "type": "date"},
            "shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "=", "type": "other"},
        }

    def test_identifier_naming_only_filter_parsed(self) -> None:
        """Identifier-type filters without column/value are accepted as naming hints."""
        kwargs = {"filters": {"shop_id": {"type": "identifier"}}}
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))
        assert result == {"shop_id": {"value": None, "operator": "=", "type": "identifier"}}

    def test_assemble_where_skips_naming_only_identifier(self) -> None:
        """assemble_where_statement should ignore naming-only identifier filters when building WHERE."""
        filters = {
            "shop_id": {"type": "identifier", "value": None},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        }
        where_sql = DataQualityCheck.assemble_where_statement(filters)
        assert "shop_id" not in where_sql
        assert "DATE" in where_sql

    def test_identifier_placeholder_used(self) -> None:
        """When identifier filter has no value, identifier_placeholder is used in check identifier and column naming."""

        class DummyCheck(DataQualityCheck):
            def assemble_query(self) -> str:
                return "SELECT 1"

            def assemble_data_exists_query(self) -> str:
                return "SELECT '' AS empty_table"

            def assemble_name(self) -> str:
                return "dummy"

        chk = DummyCheck(
            database_accessor="",
            database_provider=None,
            table="t",
            filters={"shop_id": {"type": "identifier"}},
            identifier_format="filter_name",
            identifier_placeholder="PLACEHOLDER",
        )
        assert chk.identifier == "PLACEHOLDER"
        assert chk.identifier_column == "SHOP_ID"


class TestOperatorFilters:
    """Tests for filter operator functionality."""

    def test_equality_operator_default(self) -> None:
        """Test that equality operator is used by default."""
        kwargs = {
            "filters": {
                "shop_id": {"column": "shop_code", "value": "SHOP001"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "=", "type": "other"}}

    def test_greater_than_operator(self) -> None:
        """Test greater than operator."""
        kwargs = {
            "filters": {
                "revenue": {"column": "total_revenue", "value": 1000, "operator": ">"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"revenue": {"column": "total_revenue", "value": 1000, "operator": ">", "type": "other"}}

    def test_greater_than_or_equal_operator(self) -> None:
        """Test greater than or equal operator."""
        kwargs = {
            "filters": {
                "revenue": {"column": "total_revenue", "value": 1000, "operator": ">="},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"revenue": {"column": "total_revenue", "value": 1000, "operator": ">=", "type": "other"}}

    def test_less_than_operator(self) -> None:
        """Test less than operator."""
        kwargs = {
            "filters": {
                "count": {"column": "item_count", "value": 100, "operator": "<"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"count": {"column": "item_count", "value": 100, "operator": "<", "type": "other"}}

    def test_not_equal_operator(self) -> None:
        """Test not equal operator."""
        kwargs = {
            "filters": {
                "status": {"column": "order_status", "value": "cancelled", "operator": "!="},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"status": {"column": "order_status", "value": "cancelled", "operator": "!=", "type": "other"}}

    def test_in_operator_with_list(self) -> None:
        """Test IN operator with list of values."""
        kwargs = {
            "filters": {
                "category": {"column": "category", "value": ["toys", "electronics"], "operator": "IN"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {
            "category": {"column": "category", "value": ["toys", "electronics"], "operator": "IN", "type": "other"},
        }

    def test_not_in_operator(self) -> None:
        """Test NOT IN operator."""
        kwargs = {
            "filters": {
                "category": {"column": "category", "value": ["returns", "refunds"], "operator": "NOT IN"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {
            "category": {"column": "category", "value": ["returns", "refunds"], "operator": "NOT IN", "type": "other"},
        }

    def test_like_operator(self) -> None:
        """Test LIKE operator."""
        kwargs = {
            "filters": {
                "name": {"column": "product_name", "value": "%widget%", "operator": "LIKE"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {"name": {"column": "product_name", "value": "%widget%", "operator": "LIKE", "type": "other"}}

    def test_mixed_operators(self) -> None:
        """Test multiple filters with different operators."""
        kwargs = {
            "filters": {
                "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
                "revenue": {"column": "total_revenue", "value": 1000, "operator": ">="},
                "category": {"column": "category", "value": ["toys", "electronics"], "operator": "IN"},
            },
        }
        result = DataQualityCheck.get_filters(kwargs.get("filters", {}))

        assert result == {
            "date": {"column": "DATE", "value": "2023-01-01", "operator": "=", "type": "date"},
            "revenue": {"column": "total_revenue", "value": 1000, "operator": ">=", "type": "other"},
            "category": {"column": "category", "value": ["toys", "electronics"], "operator": "IN", "type": "other"},
        }


class TestAssembleWhereStatement:
    """Tests for the assemble_where_statement method."""

    def test_empty_filters(self) -> None:
        """Test with empty filters."""
        result = DataQualityCheck.assemble_where_statement({})
        assert result == ""

    def test_equality_filter(self) -> None:
        """Test equality filter SQL generation."""
        filters = {"shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "="}}
        result = DataQualityCheck.assemble_where_statement(filters)

        assert result == "WHERE\n    shop_code = 'SHOP001'"

    def test_numeric_filter(self) -> None:
        """Test numeric filter SQL generation."""
        filters = {"revenue": {"column": "total_revenue", "value": 1000, "operator": ">="}}
        result = DataQualityCheck.assemble_where_statement(filters)

        assert result == "WHERE\n    total_revenue >= 1000"

    def test_in_operator_sql(self) -> None:
        """Test IN operator SQL generation."""
        filters = {"category": {"column": "category", "value": ["toys", "electronics"], "operator": "IN"}}
        result = DataQualityCheck.assemble_where_statement(filters)

        assert result == "WHERE\n    category IN ('toys', 'electronics')"

    def test_not_in_operator_sql(self) -> None:
        """Test NOT IN operator SQL generation."""
        filters = {"category": {"column": "category", "value": ["returns"], "operator": "NOT IN"}}
        result = DataQualityCheck.assemble_where_statement(filters)

        assert result == "WHERE\n    category NOT IN ('returns')"

    def test_like_operator_sql(self) -> None:
        """Test LIKE operator SQL generation."""
        filters = {"name": {"column": "product_name", "value": "%widget%", "operator": "LIKE"}}
        result = DataQualityCheck.assemble_where_statement(filters)

        assert result == "WHERE\n    product_name LIKE '%widget%'"

    def test_multiple_filters_sql(self) -> None:
        """Test multiple filters in SQL WHERE clause."""
        filters = {
            "shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "="},
            "revenue": {"column": "total_revenue", "value": 1000, "operator": ">="},
        }
        result = DataQualityCheck.assemble_where_statement(filters)

        assert "WHERE\n" in result
        assert "shop_code = 'SHOP001'" in result
        assert "total_revenue >= 1000" in result
        assert "\nAND\n" in result

    def test_null_value_is_null(self) -> None:
        """Test that None value generates IS NULL."""
        filters = {"deleted": {"column": "deleted_at", "value": None, "operator": "="}}
        result = DataQualityCheck.assemble_where_statement(filters)

        assert result == "WHERE\n    deleted_at IS NULL"

    def test_null_value_is_not_null(self) -> None:
        """Test that None value with != operator generates IS NOT NULL."""
        filters = {"active": {"column": "deleted_at", "value": None, "operator": "!="}}
        result = DataQualityCheck.assemble_where_statement(filters)

        assert result == "WHERE\n    deleted_at IS NOT NULL"

    def test_null_with_other_filters(self) -> None:
        """Test NULL filter combined with other filters."""
        filters = {
            "shop_id": {"column": "shop_code", "value": "SHOP001", "operator": "="},
            "active": {"column": "deleted_at", "value": None, "operator": "="},
        }
        result = DataQualityCheck.assemble_where_statement(filters)

        assert "WHERE\n" in result
        assert "shop_code = 'SHOP001'" in result
        assert "deleted_at IS NULL" in result
        assert "\nAND\n" in result


class TestFilterTypeValidation:
    """Tests for filter type validation."""

    def test_single_date_filter_allowed(self) -> None:
        """Test that a single date filter is valid."""
        defaults = _Defaults(
            filters={
                "partition_date": FilterConfig(column="DATE", value="2023-01-01", type="date"),
                "shop_id": FilterConfig(column="shop_code", value="SHOP001"),
            },
        )
        assert len([f for f in defaults.filters.values() if f.type == "date"]) == 1

    def test_multiple_date_filters_rejected(self) -> None:
        """Test that multiple date filters raise an error."""
        with pytest.raises(ValueError, match="Only one filter with type='date' is allowed"):
            _Defaults(
                filters={
                    "date1": FilterConfig(column="DATE", value="2023-01-01", type="date"),
                    "date2": FilterConfig(column="BQ_PARTITIONTIME", value="2023-01-02", type="date"),
                },
            )

    def test_no_date_filter_allowed(self) -> None:
        """Test that having no date filter is valid."""
        defaults = _Defaults(
            filters={
                "shop_id": FilterConfig(column="shop_code", value="SHOP001"),
            },
        )
        assert len([f for f in defaults.filters.values() if f.type == "date"]) == 0

    def test_multiple_date_type_filters_rejected(self) -> None:
        """Test that having two filters with type='date' raises an error."""
        with pytest.raises(ValueError, match="Only one filter with type='date' is allowed"):
            _Defaults(
                filters={
                    "partition_date": FilterConfig(column="BQ_PARTITIONTIME", value="yesterday", type="date"),
                    "order_date": FilterConfig(column="ORDER_DATE", value="today", type="date"),
                    "shop_id": FilterConfig(column="shop_code", value="SHOP001"),
                },
            )

    def test_date_type_and_parse_as_date_allowed(self) -> None:
        """Test that one type='date' filter and another with parse_as_date=True is valid."""
        # This should NOT raise - only type="date" counts toward the limit
        defaults = _Defaults(
            filters={
                "partition_date": FilterConfig(column="BQ_PARTITIONTIME", value="yesterday", type="date"),
                "created_at": FilterConfig(column="created_date", value="today", parse_as_date=True),
                "shop_id": FilterConfig(column="shop_code", value="SHOP001"),
            },
        )

        # Only one filter with type="date"
        assert len([f for f in defaults.filters.values() if f.type == "date"]) == 1
        # But two filters that will have their values parsed as dates
        assert defaults.filters["partition_date"].type == "date"
        assert defaults.filters["created_at"].parse_as_date is True
        assert defaults.filters["created_at"].type == "other"

    def test_single_identifier_filter_allowed(self) -> None:
        """Test that a single identifier filter is valid."""
        defaults = _Defaults(
            filters={
                "shop_id": FilterConfig(column="shop_code", value="SHOP001", type="identifier"),
                "date": FilterConfig(column="DATE", value="2023-01-01", type="date"),
            },
        )
        assert len([f for f in defaults.filters.values() if f.type == "identifier"]) == 1

    def test_multiple_identifier_filters_rejected(self) -> None:
        """Test that multiple identifier filters raise an error."""
        with pytest.raises(ValueError, match="Only one filter with type='identifier' is allowed"):
            _Defaults(
                filters={
                    "shop_id": FilterConfig(column="shop_code", value="SHOP001", type="identifier"),
                    "tenant_id": FilterConfig(column="tenant_code", value="TENANT01", type="identifier"),
                },
            )

    def test_date_and_identifier_filters_allowed(self) -> None:
        """Test that one date and one identifier filter together are valid."""
        defaults = _Defaults(
            filters={
                "partition_date": FilterConfig(column="BQ_PARTITIONTIME", value="yesterday", type="date"),
                "shop_id": FilterConfig(column="shop_code", value="SHOP001", type="identifier"),
                "status": FilterConfig(column="status", value="active"),
            },
        )
        assert len([f for f in defaults.filters.values() if f.type == "date"]) == 1
        assert len([f for f in defaults.filters.values() if f.type == "identifier"]) == 1
        assert len([f for f in defaults.filters.values() if f.type == "other"]) == 1


class TestOperatorValueValidation:
    """Tests for operator and value type validation."""

    def test_list_value_with_in_operator_valid(self) -> None:
        """Test that list values are valid with IN operator."""
        config = FilterConfig(column="category", value=["a", "b"], operator="IN")
        assert config.value == ["a", "b"]

    def test_list_value_with_not_in_operator_valid(self) -> None:
        """Test that list values are valid with NOT IN operator."""
        config = FilterConfig(column="category", value=["a", "b"], operator="NOT IN")
        assert config.value == ["a", "b"]

    def test_list_value_with_equality_operator_rejected(self) -> None:
        """Test that list values are rejected with = operator."""
        with pytest.raises(ValueError, match="List values can only be used with IN/NOT IN"):
            FilterConfig(column="category", value=["a", "b"], operator="=")

    def test_list_value_with_comparison_operator_rejected(self) -> None:
        """Test that list values are rejected with comparison operators."""
        with pytest.raises(ValueError, match="List values can only be used with IN/NOT IN"):
            FilterConfig(column="amount", value=[1, 2, 3], operator=">=")

    def test_in_operator_with_scalar_value_rejected(self) -> None:
        """Test that IN operator requires a list value."""
        with pytest.raises(ValueError, match="IN/NOT IN operators require a list value"):
            FilterConfig(column="category", value="single", operator="IN")

    def test_not_in_operator_with_scalar_value_rejected(self) -> None:
        """Test that NOT IN operator requires a list value."""
        with pytest.raises(ValueError, match="IN/NOT IN operators require a list value"):
            FilterConfig(column="category", value="single", operator="NOT IN")

    def test_null_value_with_equality_operator_valid(self) -> None:
        """Test that null value is valid with = operator."""
        config = FilterConfig(column="deleted_at", value=None, operator="=")
        assert config.value is None

    def test_null_value_with_not_equal_operator_valid(self) -> None:
        """Test that null value is valid with != operator."""
        config = FilterConfig(column="deleted_at", value=None, operator="!=")
        assert config.value is None

    def test_null_value_with_comparison_operator_rejected(self) -> None:
        """Test that null value is rejected with comparison operators."""
        with pytest.raises(ValueError, match="None/null values can only be used with = or !="):
            FilterConfig(column="amount", value=None, operator=">=")

    def test_null_value_with_in_operator_rejected(self) -> None:
        """Test that null value is rejected with IN operator."""
        with pytest.raises(ValueError, match="None/null values can only be used with = or !="):
            FilterConfig(column="category", value=None, operator="IN")


class TestIdentifierFormatValidation:
    """Tests for identifier format consistency validation."""

    def test_identifier_format_identifier_allows_different_filter_names(self) -> None:
        """Test that identifier format allows different filter names."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  identifier_format: identifier
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
    checks:
      - filters:
          shop_id:
            column: shop_code
            value: SHOP1
            type: identifier
  - name: bundle2
    defaults:
      check_type: CountCheck
      table: t2
      check_column: "*"
    checks:
      - filters:
          tenant_id:
            column: tenant_code
            value: TENANT1
            type: identifier
"""
        # Should not raise - identifier format allows different names
        config = parse_yaml_raw_as(Config, yaml_config)
        assert config.defaults.identifier_format == "identifier"

    def test_filter_name_format_rejects_different_filter_names(self) -> None:
        """Test that filter_name format rejects different filter names."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  identifier_format: filter_name
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
    checks:
      - filters:
          shop_id:
            column: shop_code
            value: SHOP1
            type: identifier
  - name: bundle2
    defaults:
      check_type: CountCheck
      table: t2
      check_column: "*"
    checks:
      - filters:
          tenant_id:
            column: tenant_code
            value: TENANT1
            type: identifier
"""
        with pytest.raises(ValueError, match="all identifier filters must have the same filter name"):
            parse_yaml_raw_as(Config, yaml_config)

    def test_filter_name_format_allows_same_filter_names(self) -> None:
        """Test that filter_name format allows same filter names."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  identifier_format: filter_name
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
    checks:
      - filters:
          shop_id:
            column: shop_code
            value: SHOP1
            type: identifier
  - name: bundle2
    defaults:
      check_type: CountCheck
      table: t2
      check_column: "*"
    checks:
      - filters:
          shop_id:
            column: different_column
            value: SHOP2
            type: identifier
"""
        # Should not raise - same filter name
        config = parse_yaml_raw_as(Config, yaml_config)
        assert config.defaults.identifier_format == "filter_name"

    def test_column_name_format_rejects_different_column_names(self) -> None:
        """Test that column_name format rejects different column names."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  identifier_format: column_name
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
    checks:
      - filters:
          shop_id:
            column: shop_code
            value: SHOP1
            type: identifier
  - name: bundle2
    defaults:
      check_type: CountCheck
      table: t2
      check_column: "*"
    checks:
      - filters:
          shop_id:
            column: tenant_code
            value: TENANT1
            type: identifier
"""
        with pytest.raises(ValueError, match="all identifier filters must have the same column name"):
            parse_yaml_raw_as(Config, yaml_config)

    def test_column_name_format_allows_same_column_names(self) -> None:
        """Test that column_name format allows same column names."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  identifier_format: column_name
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
    checks:
      - filters:
          shop_id:
            column: shop_code
            value: SHOP1
            type: identifier
  - name: bundle2
    defaults:
      check_type: CountCheck
      table: t2
      check_column: "*"
    checks:
      - filters:
          different_name:
            column: shop_code
            value: SHOP2
            type: identifier
"""
        # Should not raise - same column name
        config = parse_yaml_raw_as(Config, yaml_config)
        assert config.defaults.identifier_format == "column_name"


class TestFilterValueValidation:
    """Tests for filter value completeness validation."""

    def test_filter_missing_value_in_check_rejected(self) -> None:
        """Test that filters without values in checks are rejected."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  filters:
    shop_id:
      column: shop_code
      type: identifier
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
    checks:
      - {}
"""
        with pytest.raises(ValueError, match="is missing a value"):
            parse_yaml_raw_as(Config, yaml_config)

    def test_filter_value_in_bundle_defaults_accepted(self) -> None:
        """Test that filter value set in bundle defaults is accepted."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  filters:
    shop_id:
      column: shop_code
      type: identifier
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
      filters:
        shop_id:
          value: SHOP001
    checks:
      - {}
"""
        config = parse_yaml_raw_as(Config, yaml_config)
        assert config.check_bundles[0].checks[0].filters["shop_id"].value == "SHOP001"

    def test_filter_value_in_check_accepted(self) -> None:
        """Test that filter value set in check is accepted."""
        yaml_config = """
name: test
database_setup: ""
database_accessor: ""
defaults:
  filters:
    shop_id:
      column: shop_code
      type: identifier
check_bundles:
  - name: bundle1
    defaults:
      check_type: CountCheck
      table: t1
      check_column: "*"
    checks:
      - filters:
          shop_id:
            value: SHOP001
"""
        config = parse_yaml_raw_as(Config, yaml_config)
        assert config.check_bundles[0].checks[0].filters["shop_id"].value == "SHOP001"
