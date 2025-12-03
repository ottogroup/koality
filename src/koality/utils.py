"""Utils for big expectations"""

import re
from ast import literal_eval
from collections.abc import Iterable
from importlib import import_module
from typing import Any, Union

import numpy as np
import pandas as pd
from pandas._libs.missing import NAType


def parse_gcs_path(path: str) -> tuple[str, str]:
    """Parses a path of the form gs://mybucket/my/blob"""
    if not path.startswith("gs://"):
        raise ValueError("A GCS path needs to start with gs://")

    bucket_name = path[len("gs://") :].split("/")[0]
    blob_name = path[len("gs://") + len(bucket_name) + 1 :]

    if not blob_name:
        raise ValueError("Blob name is empty")

    return bucket_name, blob_name


def resolve_dotted_name(dotted_name: str) -> object:
    """
    Resolves a dotted name, e.g., pointing to a class or function and
    returns the corresponding object.

    Args:
        dotted_name: A dotted path referring to a specific resource in a module.

    Returns
        An object (e.g., class) of a module.

    """
    if ":" in dotted_name:
        module, name = dotted_name.split(":")
    elif "." in dotted_name:
        module, name = dotted_name.rsplit(".", 1)
    else:
        module, name = "koality.checks", dotted_name

    attr = import_module(module)
    if name:
        for n in name.split("."):
            attr = getattr(attr, n)

    return attr


def parse_date(date: Union[str, int], offset_days: int = 0) -> str:
    """
    Parses a date string which can be a relative terms like "today", "yesterday",
    or "tomorrow", actual dates, or relative dates like "today-2".

    Args:
        date: The date string to be parsed.
        offset_days: The number of days to be added/substracted.
    """
    date = str(date).lower()
    if date == "yesterday":
        offset_days -= 1
        return (pd.Timestamp("today") + pd.Timedelta(days=offset_days)).date().isoformat()

    if date == "tomorrow":
        offset_days += 1
        return (pd.Timestamp("today") + pd.Timedelta(days=offset_days)).date().isoformat()

    if regex_match := re.search(r"today([+-][0-9]+)", date):
        offset_days += int(regex_match[1])
        return (pd.Timestamp("today") + pd.Timedelta(days=offset_days)).date().isoformat()

    return (pd.Timestamp(date) + pd.Timedelta(days=offset_days)).date().isoformat()


def parse_arg(arg: str) -> Union[str, int, bool]:
    if arg.lower() == "false":
        return False
    if arg.lower() == "true":
        return True

    if re.fullmatch(r"\d+(\.\d+)?", arg):  # if is int or float
        return literal_eval(arg)

    return arg


def to_set(value: Any) -> set:
    """
    Converts the input string to a set. The special case of one single string
    is also covered. Duplicates are also removed and for deterministic behavior,
    the values are sorted.

    It will, convert input as follows:
    - 1 -> {1}
    - True -> {True}
    - "toys" / '"toys"' -> {"toys"}
    - ("toys") / '("toys")' -> {"toys"}
    - ("toys", "shirt") / '("toys", "shirt")' -> {"shirt", "toys"}
    - ["toys"] -> {"toys"}
    - {"toys"} -> {"toys"}

    """
    try:
        value = literal_eval(value)
    except ValueError:
        pass
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        return {value}
    if isinstance(value, set):
        return value
    return set(value)


def format_dynamic(value: Union[int, float, None, NAType], min_precision: int = 4) -> str:
    """
    Rounds a numeric value to min_precision decimals or more if needed in order to get
    a non-zero result and returns it a string, e.g.:

    - 0.1234      -> "0.1234"
    - 0.00001     -> "0.00001"
    - 0.000010123 -> "0.00001"
    - 0.1         -> "0.1"

    Args:
        value: Number to be processed.
        min_precision: Minimum number of decimals to be used.

    Returns:
        A rounded string representation of the value.
    """
    if pd.isna(value) or value is None:
        value = np.nan

    if value == 0:
        return "0"

    if min_precision < 1:
        raise ValueError("min_precision must be >= 1")

    prec = int(min_precision)

    while (rounded_value := round(value, prec)) == 0:
        prec += 1

    return np.format_float_positional(rounded_value, trim="-")
