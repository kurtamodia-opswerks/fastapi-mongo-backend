import uuid
import pandas as pd
import numpy as np
from typing import Literal, Dict

# shorter UUID format: xxxx-xxxx-xxxx
def generate_short_uuid():
    u = uuid.uuid4().hex  # 32 hex chars
    return f"{u[:4]}-{u[4:8]}-{u[8:12]}"

FieldType = Literal["numeric", "categorical", "date", "boolean", "unknown"]

def detect_column_type(series: pd.Series) -> FieldType:
    """Detects column type similar to frontend version"""
    non_null = series.dropna().astype(str)

    if non_null.empty:
        return "unknown"

    # Date detection
    try:
        parsed_dates = pd.to_datetime(non_null, errors="coerce")
        if parsed_dates.notna().mean() > 0.7:
            return "date"
    except Exception:
        pass

    # Numeric detection
    numeric_converted = pd.to_numeric(non_null, errors="coerce")
    if numeric_converted.notna().mean() > 0.7:
        return "numeric"

    # Boolean detection
    lower_vals = non_null.str.lower().unique().tolist()
    bool_set = {"true", "false", "yes", "no", "0", "1"}
    if all(v in bool_set for v in lower_vals):
        return "boolean"

    return "categorical"
