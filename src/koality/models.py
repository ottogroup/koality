"""Pydantic models for koality configuration validation."""

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, computed_field, confloat, conint, model_validator

# Supported SQL comparison operators
FilterOperator = Literal["=", "!=", "<", ">", "<=", ">=", "IN", "NOT IN", "LIKE", "NOT LIKE"]

# Filter types
FilterType = Literal["date", "identifier", "other"]

# Filter value types - single values or lists for IN/NOT IN operators, None for IS NULL
FilterValue = str | int | float | datetime | list[str | int | float | datetime] | None

# Identifier column naming options for result output
# - "identifier": outputs "column=value" format (e.g., "shop_code=EC0601")
# - "filter_name": uses the filter name as column header (e.g., "shop_id")
# - "column_name": uses the database column name as header (e.g., "shop_code")
IdentifierFormat = Literal["identifier", "filter_name", "column_name"]


class FilterConfig(BaseModel):
    """Configuration for a single filter.

    Attributes:
        column: The database column name to filter on.
        value: The filter value (can be any type, will be converted to string in SQL).
            For IN/NOT IN operators, use a list of values.
            For date filters, supports relative dates like "today", "yesterday",
            and offsets like "yesterday-2" or "today+1".
        operator: SQL comparison operator. Defaults to "=" (equality).
        type: Filter type - "date" for date filters (used for rolling checks),
            "identifier" for identifier filters (e.g., shop_id),
            "other" for regular filters. Only one "date" and one "identifier" type
            filter is allowed per configuration.
            When type="date", the value is automatically parsed as a date.
        parse_as_date: If True, the value will be parsed as a date even for type="other".
            Useful for filters that need date parsing but aren't the primary date filter.

    Example:
        filters:
          partition_date:
            column: BQ_PARTITIONTIME
            value: yesterday-2  # 2 days before yesterday
            type: date
          shop_id:
            column: shopId
            value: EC0601
            type: identifier
          created_at:
            column: created_date
            value: today+1  # tomorrow
            parse_as_date: true  # parses date but doesn't count as the "date" filter
          revenue:
            column: total_revenue
            value: 1000
            operator: ">="

    """

    column: str | None = None
    value: FilterValue = None
    operator: FilterOperator = "="
    type: FilterType = "other"
    parse_as_date: bool = False

    @model_validator(mode="after")
    def validate_operator_value_combination(self) -> Self:
        """Validate that operator and value type are compatible.

        Skips validation when value is None with default operator, as this
        indicates a partial filter in defaults that will be completed later.
        """
        # Skip validation for partial filters (value not set, using default operator)
        if self.value is None and self.operator == "=":
            return self

        if self.value is None:
            if self.operator not in ("=", "!="):
                msg = f"None/null values can only be used with = or != operators, got: {self.operator}"
                raise ValueError(msg)
        elif isinstance(self.value, list):
            if self.operator not in ("IN", "NOT IN"):
                msg = f"List values can only be used with IN/NOT IN operators, got: {self.operator}"
                raise ValueError(msg)
        elif self.operator in ("IN", "NOT IN"):
            msg = f"IN/NOT IN operators require a list value, got: {type(self.value).__name__}"
            raise ValueError(msg)
        return self


# Type alias for filters dict
FilterDict = dict[str, Annotated[FilterConfig, Field(default_factory=FilterConfig)]]


@dataclass
class DatabaseProvider:
    """Data class representing a DuckDB database provider connection."""

    database_name: str
    database_oid: int
    path: str
    comment: str | None
    tags: dict
    internal: bool
    type: str
    readonly: bool
    encrypted: bool
    cipher: str | None


type CHECK_TYPE = Literal[
    "DataQualityCheck",
    "ColumnTransformationCheck",
    "NullRatioCheck",
    "RegexMatchCheck",
    "ValuesInSetCheck",
    "RollingValuesInSetCheck",
    "DuplicateCheck",
    "CountCheck",
    "AverageCheck",
    "MaxCheck",
    "MinCheck",
    "OccurrenceCheck",
    "MatchRateCheck",
    "RelCountChangeCheck",
    "IqrOutlierCheck",
]

type CHECK = (
    _NullRatioCheck
    | _RegexMatchCheck
    | _ValuesInSetCheck
    | _RollingValuesInSetCheck
    | _DuplicateCheck
    | _CountCheck
    | _AverageCheck
    | _MaxCheck
    | _MinCheck
    | _OccurrenceCheck
    | _MatchRateCheck
    | _RelCountChangeCheck
    | _IqrOutlierCheck
)


class _Defaults(BaseModel):
    filters: dict[str, FilterConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_single_type_filters(self) -> Self:
        """Validate that there is at most one filter with type='date' and one with type='identifier'."""
        date_filters = [name for name, cfg in self.filters.items() if cfg.type == "date"]
        if len(date_filters) > 1:
            msg = f"Only one filter with type='date' is allowed, found: {date_filters}"
            raise ValueError(msg)
        identifier_filters = [name for name, cfg in self.filters.items() if cfg.type == "identifier"]
        if len(identifier_filters) > 1:
            msg = f"Only one filter with type='identifier' is allowed, found: {identifier_filters}"
            raise ValueError(msg)
        return self


class _LocalDefaults(_Defaults):
    check_type: CHECK_TYPE | None = None
    check_column: str | None = None
    lower_threshold: float = -math.inf
    upper_threshold: float = math.inf
    right_table: str | None = None
    left_table: str | None = None


class _GlobalDefaults(_Defaults):
    monitor_only: bool = False
    result_table: str | None = None
    identifier_format: IdentifierFormat = "identifier"

    @computed_field
    def persist_results(self) -> bool:
        return self.result_table is not None

    log_path: str | None = None


class _Check(_LocalDefaults):
    """Base model for all check configurations."""


class _SingleTableCheck(_Check):
    """Base model for checks that operate on a single table."""

    table: str


class _NullRatioCheck(_SingleTableCheck):
    """Config model for NullRatioCheck."""

    check_type: Literal["NullRatioCheck"]


class _RegexMatchCheck(_SingleTableCheck):
    """Config model for RegexMatchCheck."""

    check_type: Literal["RegexMatchCheck"]
    regex_to_match: str


class _ValuesInSetCheck(_SingleTableCheck):
    """Config model for ValuesInSetCheck."""

    check_type: Literal["ValuesInSetCheck"]
    value_set: list[str] | str


class _RollingValuesInSetCheck(_ValuesInSetCheck):
    """Config model for RollingValuesInSetCheck."""

    check_type: Literal["RollingValuesInSetCheck"]


class _DuplicateCheck(_SingleTableCheck):
    """Config model for DuplicateCheck."""

    check_type: Literal["DuplicateCheck"]


class _CountCheck(_SingleTableCheck):
    """Config model for CountCheck."""

    check_type: Literal["CountCheck"]
    distinct: bool = False


class _AverageCheck(_SingleTableCheck):
    """Config model for AverageCheck."""

    check_type: Literal["AverageCheck"]


class _MaxCheck(_SingleTableCheck):
    """Config model for MaxCheck."""

    check_type: Literal["MaxCheck"]


class _MinCheck(_SingleTableCheck):
    """Config model for MinCheck."""

    check_type: Literal["MinCheck"]


class _OccurrenceCheck(_SingleTableCheck):
    """Config model for OccurrenceCheck."""

    check_type: Literal["OccurrenceCheck"]
    max_or_min: Literal["max", "min"]


class _MatchRateCheck(_Check):
    """Config model for MatchRateCheck."""

    check_type: Literal["MatchRateCheck"]
    left_table: str
    right_table: str
    join_columns: list[str] | None = None
    join_columns_left: list[str] | None = None
    join_columns_right: list[str] | None = None

    @model_validator(mode="after")
    def validate_join_columns(self) -> Self:
        if not (self.join_columns or (self.join_columns_left and self.join_columns_right)):
            msg = "No join_columns provided. Use either join_columns or join_columns_left and join_columns_right"
            raise ValueError(msg)
        if (
            self.join_columns_left
            and self.join_columns_right
            and len(self.join_columns_left) != len(self.join_columns_right)
        ):
            msg = (
                f"join_columns_left and join_columns_right must have equal length "
                f"({len(self.join_columns_left)} vs. {len(self.join_columns_right)})"
            )
            raise ValueError(msg)
        return self


class _RelCountChangeCheck(_SingleTableCheck):
    """Config model for RelCountChangeCheck."""

    rolling_days: conint(ge=1)


class _IqrOutlierCheck(_SingleTableCheck):
    """Config model for IqrOutlierCheck."""

    interval_days: conint(ge=1)
    how: Literal["both", "upper", "lower"]
    iqr_factor: confloat(gt=0)


class _CheckBundle(BaseModel):
    name: str
    defaults: _LocalDefaults = Field(default_factory=_LocalDefaults)
    checks: list[CHECK]


class Config(BaseModel):
    """Root configuration model for koality check execution."""

    name: str
    database_setup: str
    database_accessor: str
    defaults: _GlobalDefaults
    check_bundles: list[_CheckBundle]

    @model_validator(mode="before")
    @classmethod
    def propagate_defaults_to_checks(cls, data: dict) -> dict:
        """Merge defaults and check_bundle.defaults into each check before validation.

        Merge order (later overrides earlier):
        1. defaults
        2. bundle defaults
        3. check-specific values

        For the 'filters' dict, a deep merge is performed so that check-level
        filters override individual filter entries rather than replacing the whole dict.
        """
        if not isinstance(data, dict):
            return data

        defaults = data.get("defaults", {})
        check_bundles = data.get("check_bundles", [])

        if not check_bundles:
            return data

        updated_bundles = []
        for bundle in check_bundles:
            if not isinstance(bundle, dict):
                updated_bundles.append(bundle)
                continue

            bundle_defaults = bundle.get("defaults", {})
            checks = bundle.get("checks", [])

            merged_checks = []
            for check in checks:
                if isinstance(check, dict):
                    # Merge order: defaults -> check_bundle.defaults -> check
                    merged = {**defaults, **bundle_defaults, **check}

                    # Deep merge for 'filters' dict
                    merged["filters"] = cls._merge_filters(
                        defaults.get("filters", {}),
                        bundle_defaults.get("filters", {}),
                        check.get("filters", {}),
                    )

                    merged_checks.append(merged)
                else:
                    merged_checks.append(check)

            bundle["checks"] = merged_checks
            updated_bundles.append(bundle)

        data["check_bundles"] = updated_bundles
        return data

    @staticmethod
    def _merge_filters(*filter_dicts: dict) -> dict:
        """Deep merge multiple filter dicts.

        For each filter name, later values override earlier ones.
        Within a single filter, individual keys are merged (e.g., value overrides
        but column is inherited if not specified).
        """
        result: dict = {}
        for filters in filter_dicts:
            if not filters:
                continue
            for name, config in filters.items():
                if name not in result:
                    result[name] = config if isinstance(config, dict) else {"value": config}
                elif isinstance(config, dict):
                    result[name] = {**result[name], **config}
                else:
                    # Shorthand: just a value
                    result[name]["value"] = config
        return result

    @model_validator(mode="after")
    def validate_identifier_consistency(self) -> Self:
        """Validate identifier filter consistency based on identifier_format.

        When identifier_format is 'filter_name' or 'column_name', all identifier
        filters across all checks must have the same filter name or column name
        respectively, since these are used as result column headers.
        """
        identifier_format = self.defaults.identifier_format
        if identifier_format == "identifier":
            return self

        filter_names: set[str] = set()
        column_names: set[str] = set()

        for bundle in self.check_bundles:
            for check in bundle.checks:
                for name, config in check.filters.items():
                    if config.type == "identifier":
                        filter_names.add(name)
                        column_names.add(config.column)

        if identifier_format == "filter_name" and len(filter_names) > 1:
            msg = (
                f"When identifier_format='filter_name', all identifier filters must have "
                f"the same filter name. Found different names: {sorted(filter_names)}"
            )
            raise ValueError(msg)

        if identifier_format == "column_name" and len(column_names) > 1:
            msg = (
                f"When identifier_format='column_name', all identifier filters must have "
                f"the same column name. Found different columns: {sorted(column_names)}"
            )
            raise ValueError(msg)

        return self

    @model_validator(mode="after")
    def validate_filter_values_complete(self) -> Self:
        """Validate that all filters in checks have column and value set.

        Filters in defaults can omit column/value (to be set at check level),
        but after merging, all filters must have both column and value.
        """
        for bundle in self.check_bundles:
            for check in bundle.checks:
                for name, config in check.filters.items():
                    if config.column is None:
                        msg = (
                            f"Filter '{name}' in check '{check.check_type}' "
                            f"(bundle '{bundle.name}') is missing a column. "
                            f"Set column in defaults, bundle defaults, or the check itself."
                        )
                        raise ValueError(msg)
                    if config.value is None and config.operator == "=":
                        msg = (
                            f"Filter '{name}' in check '{check.check_type}' "
                            f"(bundle '{bundle.name}') is missing a value. "
                            f"Set value in defaults, bundle defaults, or the check itself."
                        )
                        raise ValueError(msg)
        return self
