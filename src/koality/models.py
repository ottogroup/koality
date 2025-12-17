"""Pydantic models for koality configuration validation."""

from dataclasses import dataclass
from typing import Literal, Self

from pydantic import BaseModel, Field, computed_field, confloat, conint, model_validator


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
    date_filter_column: str | None = None
    date_filter_value: str | None = None
    filter_column: str | None = None
    filter_value: str | None = None


class _LocalDefaults(_Defaults):
    check_type: CHECK_TYPE | None = None
    check_column: str | None = None
    lower_threshold: float | Literal["-infinity", "infinity"] = "-infinity"
    upper_threshold: float | Literal["-infinity", "infinity"] = "infinity"
    right_table: str | None = None
    left_table: str | None = None


class _GlobalDefaults(_Defaults):
    monitor_only: bool = False
    result_table: str | None = None

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

    check_type: Literal["NullRatioCheck"]
    regex_to_match: str


class _ValuesInSetCheck(_SingleTableCheck):
    """Config model for ValuesInSetCheck."""

    check_type: Literal["ValuesInSetCheck"]
    value_set: list[str] | str


class _RollingValuesInSetCheck(_ValuesInSetCheck):
    """Config model for RollingValuesInSetCheck."""

    check_type: Literal["RollingValuesInSetCheck"]
    date_filter_column: str
    date_filter_value: str


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
    date_filter_column: str
    date_filter_value: str


class _IqrOutlierCheck(_SingleTableCheck):
    """Config model for IqrOutlierCheck."""

    date_filter_column: str
    date_filter_value: str
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
                    merged_checks.append(merged)
                else:
                    merged_checks.append(check)

            bundle["checks"] = merged_checks
            updated_bundles.append(bundle)

        data["check_bundles"] = updated_bundles
        return data
