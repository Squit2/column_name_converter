"""
converter.py
============
WMS Order File Converter - Core Engine

Changes from review:
  - logging.basicConfig removed from module level; named logger only
  - validate() rewritten with vectorised pandas operations (no iterrows)
  - clean_data() nan handling fixed; select_dtypes uses 'str' not 'object'
  - apply_mapping() deduplicated — single code path for both case modes
  - load_customer_config() detects and raises on duplicate customer_column entries
  - customer_name now read from optional 'customer_name' row in config;
    falls back to filename-derived value if not present
  - date_format now read from optional 'date_format' row in config;
    falls back to dayfirst=True if not present
  - cleanup_output_dir() added for output folder retention management
"""

import logging
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd

# ─── LOGGING ─────────────────────────────────────────────────────────────────
# Use a named logger only. Never call basicConfig from a library module —
# that reconfigures the root logger for the entire process including the
# caller (Streamlit, tests, other scripts).

log = logging.getLogger(__name__)

if not log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    ))
    log.addHandler(_handler)
    log.setLevel(logging.INFO)
    log.propagate = False

# ─── CONFIGURATION ───────────────────────────────────────────────────────────

WMS_FIELDS = {
    "ORDER_REF":        {"mandatory": True,  "dtype": str},
    "ORDER_DATE":       {"mandatory": True,  "dtype": str},
    "CUST_CODE":        {"mandatory": True,  "dtype": str},
    "CUST_NAME":        {"mandatory": False, "dtype": str},
    "ADDRESS_NO":       {"mandatory": True,  "dtype": str},
    "ADDRESS1":         {"mandatory": False, "dtype": str},
    "PROD_CODE":        {"mandatory": True,  "dtype": str},
    "ORIG_QTY_ORDERED": {"mandatory": True,  "dtype": float},
    "ORIG_UOM_CODE":    {"mandatory": True,  "dtype": str},
    "AOPT_DEC_FIELD3":  {"mandatory": False, "dtype": float},
    "AOPT_DEC_FIELD4":  {"mandatory": False, "dtype": float},
    "LOT_NO":           {"mandatory": False, "dtype": str},
    "REMARKS2":         {"mandatory": False, "dtype": str},
    "CUST_PO":          {"mandatory": False, "dtype": str},
    "AOPT_FIELD4":      {"mandatory": False, "dtype": str},
    "AOPT_FIELD5":      {"mandatory": False, "dtype": str},
    "REMARKS":          {"mandatory": False, "dtype": str},
}

MANDATORY_FIELDS = [f for f, m in WMS_FIELDS.items() if m["mandatory"]]
ALL_WMS_FIELDS   = list(WMS_FIELDS.keys())

# Numeric fields that require float-parseable values
NUMERIC_FIELDS = ("ORIG_QTY_ORDERED", "AOPT_DEC_FIELD3", "AOPT_DEC_FIELD4")

MAPPINGS_DIR = Path(__file__).parent / "mappings"
OUTPUT_DIR   = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Maximum size allowed for an uploaded config file (1 MB)
MAX_CONFIG_BYTES = 1 * 1024 * 1024


# ─── STEP 1: LOAD CONFIG ─────────────────────────────────────────────────────

def load_customer_config(customer_key: str) -> dict:
    """
    Load a customer mapping config from the mappings/ directory.

    Config format (.csv or .xlsx) — two required columns plus optional metadata rows:

        customer_column  | wms_field
        -----------------|------------------
        DocNo            | ORDER_REF
        DocDate          | ORDER_DATE
        ...

    Optional metadata rows (customer_column = reserved keyword, wms_field = value):
        __customer_name__ | Gigly Gulp Sdn Bhd
        __date_format__   | %d/%m/%Y

    If __customer_name__ is not present, the name is derived from the filename.
    If __date_format__ is not present, dayfirst=True is used as fallback.

    Parameters
    ----------
    customer_key : str
        Config filename without extension, e.g. "gigly_gulp"

    Returns
    -------
    dict:
        customer_name : str
        column_map    : dict  {customer_column: wms_field}
        date_format   : str or None
    """
    config_path = None
    for ext in (".csv", ".xlsx"):
        candidate = MAPPINGS_DIR / f"{customer_key}{ext}"
        if candidate.exists():
            config_path = candidate
            break

    if config_path is None:
        available = list_customers()
        raise FileNotFoundError(
            f"No config found for '{customer_key}' in '{MAPPINGS_DIR}'.\n"
            f"Available customers: {available}\n"
            f"Create mappings/{customer_key}.csv with columns: customer_column, wms_field"
        )

    try:
        if config_path.suffix.lower() == ".csv":
            df_config = pd.read_csv(config_path, dtype=str)
        else:
            df_config = pd.read_excel(config_path, dtype=str)
    except Exception as e:
        raise ValueError(f"Could not read config '{config_path.name}': {e}")

    # Normalise headers
    df_config.columns = [c.strip().lower() for c in df_config.columns]

    required_cols = {"customer_column", "wms_field"}
    missing_cols  = required_cols - set(df_config.columns)
    if missing_cols:
        raise ValueError(
            f"Config '{config_path.name}' is missing columns: {missing_cols}. "
            f"Required headers: customer_column, wms_field"
        )

    # Strip all values
    df_config["customer_column"] = df_config["customer_column"].astype(str).str.strip()
    df_config["wms_field"]       = df_config["wms_field"].astype(str).str.strip()

    # Extract optional metadata rows (prefixed with __)
    meta_mask    = df_config["customer_column"].str.startswith("__")
    df_meta      = df_config[meta_mask].copy()
    df_config    = df_config[~meta_mask].copy()

    # Parse metadata
    meta = dict(zip(df_meta["customer_column"], df_meta["wms_field"]))
    customer_name = meta.get("__customer_name__",
                             customer_key.replace("_", " ").title())
    date_format   = meta.get("__date_format__", None)

    # Drop blank mapping rows
    df_config = df_config.dropna(subset=["customer_column", "wms_field"])
    df_config = df_config[
        (df_config["customer_column"] != "") &
        (df_config["wms_field"]       != "") &
        (df_config["customer_column"] != "nan") &
        (df_config["wms_field"]       != "nan")
    ]

    if df_config.empty:
        raise ValueError(f"Config '{config_path.name}' has no valid mapping rows.")

    # Detect duplicate customer_column entries — last-write-wins is silent and dangerous
    dupes = df_config[df_config["customer_column"].duplicated()]["customer_column"].tolist()
    if dupes:
        raise ValueError(
            f"Config '{config_path.name}' has duplicate customer_column entries: {dupes}. "
            f"Each source column must appear only once."
        )

    # Validate all wms_field values are recognised WMS fields
    invalid_fields = [
        v for v in df_config["wms_field"].tolist()
        if v not in ALL_WMS_FIELDS
    ]
    if invalid_fields:
        raise ValueError(
            f"Config '{config_path.name}' contains unrecognised WMS fields: {invalid_fields}. "
            f"Valid fields: {ALL_WMS_FIELDS}"
        )

    column_map = dict(zip(df_config["customer_column"], df_config["wms_field"]))

    log.info(
        f"Loaded config: '{config_path.name}' — "
        f"{len(column_map)} mapping(s) for {customer_name}"
    )
    return {
        "customer_name": customer_name,
        "column_map":    column_map,
        "date_format":   date_format,
    }


def list_customers() -> list:
    """Return all available customer keys (config filenames without extension)."""
    keys = set()
    for ext in ("*.csv", "*.xlsx"):
        for p in MAPPINGS_DIR.glob(ext):
            if p.stem != "template":
                keys.add(p.stem)
    return sorted(keys)


def validate_all_customer_configs() -> dict:
    """
    Load every customer config and return a health report for each.

    Returns
    -------
    dict of {customer_key: {"errors": [...], "warnings": [...]}}
    """
    reports = {}
    for customer_key in list_customers():
        errors   = []
        warnings = []
        try:
            config     = load_customer_config(customer_key)
            mapped_wms = set(config["column_map"].values())
            for field in MANDATORY_FIELDS:
                if field not in mapped_wms:
                    warnings.append(
                        f"Mandatory field '{field}' is not mapped in this config."
                    )
        except (FileNotFoundError, ValueError) as e:
            errors.append(str(e))
        reports[customer_key] = {"errors": errors, "warnings": warnings}
    return reports


# ─── STEP 2: READ EXCEL ──────────────────────────────────────────────────────

def read_excel(file_path, sheet_name=0) -> pd.DataFrame:
    """
    Read a customer Excel order file.

    Uses keep_default_na=False so values like "NA", "N/A", "nan" are kept
    as literal strings rather than silently converted to NaN.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if file_path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError(f"Unsupported file type: '{file_path.suffix}'")
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype=str,
            keep_default_na=False,   # prevents "NA", "N/A" etc. becoming NaN silently
        )
    except Exception as e:
        raise ValueError(f"Could not read Excel: {e}")

    df.columns = [str(c).strip() for c in df.columns]
    df.replace("", pd.NA, inplace=True)   # normalise empty strings to NA
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError("Excel file has no data rows.")

    log.info(f"Read {len(df)} rows. Columns: {list(df.columns)}")
    return df


def is_valid_xlsx(file_bytes: bytes) -> bool:
    """
    Verify a file is a genuine XLSX (ZIP) or XLS file by checking its
    magic bytes, not just its extension.

    XLSX/ZIP magic: PK\\x03\\x04
    XLS (BIFF8) magic: \\xd0\\xcf\\x11\\xe0
    """
    if len(file_bytes) < 4:
        return False
    return (
        file_bytes[:4] == b"PK\x03\x04" or       # XLSX (ZIP-based)
        file_bytes[:4] == b"\xd0\xcf\x11\xe0"     # XLS (BIFF8)
    )


# ─── STEP 3: APPLY MAPPING ───────────────────────────────────────────────────

def apply_mapping(
    df: pd.DataFrame,
    column_map: dict,
    case_insensitive_source: bool = False,
) -> tuple:
    """
    Rename DataFrame columns from customer names to WMS standard names.

    Parameters
    ----------
    df : pd.DataFrame
    column_map : dict
        {customer_column: wms_field} from the config.
    case_insensitive_source : bool
        When True, incoming column names are matched case-insensitively.
        e.g. "docno" matches a config entry of "DocNo".
    """
    warnings   = []
    rename_map = {}

    # Single unified lookup — lowercased when case_insensitive_source is True
    lookup = (
        {k.lower(): v for k, v in column_map.items()}
        if case_insensitive_source
        else column_map
    )

    for col in df.columns:
        key       = col.lower() if case_insensitive_source else col
        wms_field = lookup.get(key)
        if wms_field:
            rename_map[col] = wms_field
        else:
            warnings.append(
                f"UNMAPPED: '{col}' is not in the config and will be dropped. "
                f"Add it to the config file if it is needed."
            )

    df_mapped    = df.rename(columns=rename_map)
    cols_present = [c for c in ALL_WMS_FIELDS if c in df_mapped.columns]
    return df_mapped[cols_present], warnings


# ─── STEP 4: VALIDATE ────────────────────────────────────────────────────────

def validate(df: pd.DataFrame) -> tuple:
    """
    Validate mandatory fields and numeric fields using vectorised operations.

    Row-level errors are collected per index and the DataFrame is split into
    valid rows and error rows. No Python-level row iteration (iterrows) is used.

    Returns
    -------
    df_valid       : pd.DataFrame
    df_errors      : pd.DataFrame  (with VALIDATION_ERRORS column)
    error_messages : list of str
    """
    error_messages = []
    row_errors: dict = {}   # {row_index: [list of issue strings]}

    def _add_error(idx, msg):
        row_errors.setdefault(idx, []).append(msg)

    # Check all mandatory columns exist at all
    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            error_messages.append(
                f"MISSING COLUMN: Mandatory field '{field}' not found in mapped output."
            )

    # ── Mandatory blank checks (vectorised) ──────────────────────────────────
    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            continue
        # A cell is blank if it is NA or its stripped string form is empty
        blank_mask = (
            df[field].isna() |
            df[field].astype(str).str.strip().isin(["", "nan", "None"])
        )
        for idx in df.index[blank_mask]:
            _add_error(idx, f"'{field}' is blank")

    # ── Numeric field checks (vectorised) ────────────────────────────────────
    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        # Only check rows where the value is present
        present_mask = (
            df[field].notna() &
            ~df[field].astype(str).str.strip().isin(["", "nan", "None"])
        )
        if not present_mask.any():
            continue
        # Attempt numeric conversion; coerce failures become NaN
        cleaned = (
            df.loc[present_mask, field]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        numeric_attempt = pd.to_numeric(cleaned, errors="coerce")
        bad_mask = numeric_attempt.isna()
        for idx in numeric_attempt.index[bad_mask]:
            _add_error(idx, f"'{field}' non-numeric: '{df.at[idx, field]}'")

    # ── Split valid / error rows ──────────────────────────────────────────────
    error_indices = list(row_errors.keys())
    valid_indices = [i for i in df.index if i not in error_indices]

    df_valid  = df.loc[valid_indices].copy()
    df_errors = df.loc[error_indices].copy() if error_indices else pd.DataFrame()

    if not df_errors.empty:
        df_errors["VALIDATION_ERRORS"] = df_errors.index.map(
            lambda i: " | ".join(row_errors[i])
        )

    if row_errors:
        error_messages.append(
            f"{len(row_errors)} row(s) failed validation "
            f"(Excel rows: {[i + 2 for i in error_indices]})"
        )

    log.info(f"Validation: {len(df_valid)} valid, {len(df_errors)} error row(s).")
    return df_valid, df_errors, error_messages


# ─── STEP 5: CLEAN DATA ──────────────────────────────────────────────────────

def clean_data(df: pd.DataFrame, date_format: str = None) -> pd.DataFrame:
    """
    Apply final type cleaning before export.

    - Strip whitespace from all string fields
    - Normalise residual NA representations to empty string
    - Parse ORDER_DATE using per-customer date_format if provided,
      otherwise attempt dayfirst=True as fallback
    - Cast numeric fields to float; display ORIG_QTY_ORDERED as int when whole

    Parameters
    ----------
    df : pd.DataFrame
    date_format : str or None
        strftime format string, e.g. "%d/%m/%Y". When None, pandas
        infers with dayfirst=True.
    """
    df = df.copy()

    # Strip whitespace and normalise NA strings to empty string
    # select_dtypes(include="str") is the pandas 3.x-compatible form
    for col in df.select_dtypes(include=["str", "object"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "NaN": "", "<NA>": ""})
        )

    # Date parsing — use explicit format if config provides one,
    # otherwise infer with dayfirst=True
    if "ORDER_DATE" in df.columns:
        if date_format:
            df["ORDER_DATE"] = pd.to_datetime(
                df["ORDER_DATE"], format=date_format, errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        else:
            df["ORDER_DATE"] = pd.to_datetime(
                df["ORDER_DATE"], dayfirst=True, errors="coerce"
            ).dt.strftime("%Y-%m-%d")

    # Numeric fields
    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        df[field] = pd.to_numeric(
            df[field].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

    # Display ORIG_QTY_ORDERED as integer when it has no fractional part
    if "ORIG_QTY_ORDERED" in df.columns:
        df["ORIG_QTY_ORDERED"] = df["ORIG_QTY_ORDERED"].apply(
            lambda x: int(x) if pd.notna(x) and x == int(x) else x
        )

    return df


# ─── STEP 6: EXPORT ──────────────────────────────────────────────────────────

def export_csv(
    df: pd.DataFrame,
    customer_key: str,
    output_dir: Path = OUTPUT_DIR,
) -> Path:
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"wms_output_{customer_key}_{ts}.csv"
    cols     = [c for c in ALL_WMS_FIELDS if c in df.columns]
    df[cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"Exported {len(df)} rows to '{out_path}'")
    return out_path


def export_error_report(
    df_errors: pd.DataFrame,
    customer_key: str,
    output_dir: Path = OUTPUT_DIR,
) -> Path | None:
    if df_errors.empty:
        return None
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"wms_errors_{customer_key}_{ts}.csv"
    df_errors.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.warning(f"Error report written to '{out_path}'")
    return out_path


def cleanup_output_dir(output_dir: Path = OUTPUT_DIR, keep_days: int = 90) -> int:
    """
    Delete CSV files in output_dir older than keep_days days.

    Parameters
    ----------
    output_dir : Path
    keep_days  : int  Files older than this are deleted. Default 30.

    Returns
    -------
    int  Number of files deleted.
    """
    cutoff  = datetime.now().timestamp() - (keep_days * 86400)
    deleted = 0
    for f in output_dir.glob("*.csv"):
        if f.stat().st_mtime < cutoff:
            f.unlink()
            deleted += 1
            log.info(f"Cleaned up old output file: {f.name}")
    if deleted:
        log.info(f"Cleanup complete: {deleted} file(s) removed from '{output_dir}'")
    return deleted


# ─── MAIN PIPELINE ───────────────────────────────────────────────────────────

def run_conversion(
    file_path,
    customer_key: str,
    sheet_name=0,
    auto_confirm: bool = False,
    output_dir: Path = OUTPUT_DIR,
) -> dict:
    result = {
        "success":     False,
        "output_path": None,
        "error_path":  None,
        "valid_rows":  0,
        "error_rows":  0,
        "warnings":    [],
        "errors":      [],
    }
    try:
        config              = load_customer_config(customer_key)
        df_raw              = read_excel(file_path, sheet_name)
        df_mapped, warnings = apply_mapping(df_raw, config["column_map"])
        result["warnings"].extend(warnings)
        for w in warnings:
            log.warning(w)

        df_valid, df_errors, val_errors = validate(df_mapped)
        result["errors"].extend(val_errors)
        result["valid_rows"] = len(df_valid)
        result["error_rows"] = len(df_errors)
        for e in val_errors:
            log.error(e)

        if df_valid.empty:
            result["errors"].append("No valid rows to export.")
            return result

        df_clean = clean_data(df_valid, date_format=config.get("date_format"))

        if not auto_confirm:
            print("\n" + "=" * 60)
            print("PREVIEW — First 5 rows:")
            print("=" * 60)
            print(df_clean.head(5).to_string(index=False))
            print(f"\nValid rows  : {len(df_clean)}")
            print(f"Error rows  : {len(df_errors)}")
            if warnings:
                print("\nWarnings:")
                for w in warnings:
                    print(f"  ! {w}")
            print()
            if input("Export to CSV? (yes / no): ").strip().lower() != "yes":
                print("Export cancelled.")
                return result

        result["output_path"] = export_csv(df_clean, customer_key, output_dir)
        result["error_path"]  = export_error_report(df_errors, customer_key, output_dir)
        result["success"]     = True

    except (FileNotFoundError, ValueError) as e:
        result["errors"].append(str(e))
        log.error(str(e))
    return result


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WMS Order File Converter")
    parser.add_argument("--file",     "-f", required=True,
                        help="Path to the customer Excel file (.xlsx)")
    parser.add_argument("--customer", "-c", required=True,
                        help=f"Customer config key. Available: {list_customers()}")
    parser.add_argument("--sheet",    "-s", default=0,
                        help="Sheet name or index (default: 0 = first sheet)")
    parser.add_argument("--yes",      "-y", action="store_true",
                        help="Skip manual review prompt and export immediately")
    parser.add_argument("--cleanup",  action="store_true",
                        help="Delete output files older than 30 days and exit")
    args = parser.parse_args()

    if args.cleanup:
        n = cleanup_output_dir()
        print(f"Cleanup: {n} file(s) removed.")
        return

    result = run_conversion(args.file, args.customer, args.sheet, args.yes)
    print("\n" + "=" * 60)
    print("RESULT:", "SUCCESS" if result["success"] else "FAILED")
    print(f"Valid rows : {result['valid_rows']}")
    print(f"Error rows : {result['error_rows']}")
    if result["output_path"]: print(f"Output     : {result['output_path']}")
    if result["error_path"]:  print(f"Errors     : {result['error_path']}")
    for e in result["errors"]:   print(f"  x {e}")
    for w in result["warnings"]: print(f"  ! {w}")


if __name__ == "__main__":
    main()
