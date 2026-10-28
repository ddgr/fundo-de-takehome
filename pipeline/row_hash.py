"""Row fingerprint shared by the loader and the checks."""

import hashlib

# Control characters absent from the data, so no value can mimic a column boundary or a NULL.
SEPARATOR = "\x1f"
NULL = "\x00"


def _as_text(value, sql_type: str) -> str:
    """Render by declared type, so equal values read by pymssql and by DuckDB give equal text."""
    if value is None:
        return NULL
    if sql_type == "TIMESTAMP":
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")
    if sql_type == "DATE":
        return value.isoformat()
    if sql_type in ("INTEGER", "BIGINT"):
        return str(int(value))
    if sql_type.startswith("DECIMAL("):
        scale = int(sql_type.rstrip(")").split(",")[1])
        return f"{value:.{scale}f}"
    if sql_type == "VARCHAR":
        return value
    raise ValueError(f"row_hash has no text rule for type {sql_type}")


def row_hash(values, sql_types) -> str:
    """SHA-256 of the source columns, rendered by type and joined by \\x1f."""
    text = SEPARATOR.join(_as_text(v, t) for v, t in zip(values, sql_types, strict=True))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
