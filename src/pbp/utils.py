from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def col_or(df: pd.DataFrame, name: str, default: Any) -> Any:
    """
    R-equivalent of col_or(): return df[name] if present else default.
    `default` should be broadcastable to len(df).
    """
    if name in df.columns:
        return df[name]
    return default


def as_chr(x: Any) -> pd.Series:
    """
    R-equivalent of as_chr(): safe string conversion that preserves NA as <NA>.
    """
    s = pd.Series(x)
    # Use pandas string dtype to preserve missing values as <NA>
    return s.astype("string")


def as_int(x: Any) -> pd.Series:
    """
    R-equivalent of as_int(): safe int conversion; unparsable -> <NA>.
    """
    s = pd.Series(x)
    s = pd.to_numeric(s, errors="coerce")
    # Use pandas nullable integer
    return s.astype("Int64")


def as_lgl(x: Any) -> pd.Series:
    """
    R-equivalent of as_lgl(): NA -> False.
    - If logical: NA -> False
    - If numeric: 1 -> True else False (NA -> False)
    - Else string: "1,true,t,yes" -> True
    """
    s = pd.Series(x)

    if pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool)

    if pd.api.types.is_numeric_dtype(s):
        s2 = pd.to_numeric(s, errors="coerce")
        return (s2.fillna(0) == 1).astype(bool)

    # strings
    s2 = s.astype("string").fillna("").str.lower()
    return s2.isin(["1", "true", "t", "yes"]).astype(bool)


def ensure_columns(df: pd.DataFrame, columns: list[str], fill_value: Any = pd.NA) -> pd.DataFrame:
    """
    Ensure df has all columns; if missing, add them with fill_value.
    """
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = fill_value
    return out
