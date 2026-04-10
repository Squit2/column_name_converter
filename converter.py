"""
converter.py
============
WMS Order File Converter - Core Engine
"""

import logging
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd

# ─── LOGGING ─────────────────────────────────────────────────────────────────

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
NUMERIC_FIELDS   = ("ORIG_QTY_ORDERED", "AOPT_DEC_FIELD3", "AOPT_DEC_FIELD4")

MAPPINGS_DIR     = Path(__file__).parent / "mappings"
OUTPUT_DIR       = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

MAX_CONFIG_BYTES = 1 * 1024 * 1024   # 1 MB


# ─── STEP 1: LOAD CONFIG ─────────────────────────────────────────────────────

def load_customer_config(customer_key: str) -> dict:
    """
    Load a customer mapping config from the mappings/ directory.

    Config format (.csv or .xlsx):

        customer_column | wms_field   | customer_name       | date_format
        ----------------|-------------|---------------------|------------
        DocNo           | ORDER_REF   | Gigly Gulp Sdn Bhd  | %d/%m/%Y
        DocDate         | ORDER_DATE  |                     |
        DebtorCode      | CUST_CODE   |                     |

    Required columns : customer_column, wms_field
    Optional columns : customer_name, date_format
        - customer_name : display name shown in the UI. Only needs a value on
                          the first row. If absent, derived from the filename.
        - date_format   : strftime string for ORDER_DATE, e.g. %d/%m/%Y.
                          If absent, dayfirst=True is used as fallback.
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
            "No config found for '{}' in '{}'.\n"
            "Available customers: {}\n"
            "Create mappings/{}.csv with columns: customer_column, wms_field".format(
                customer_key, MAPPINGS_DIR, available, customer_key
            )
        )

    try:
        if config_path.suffix.lower() == ".csv":
            df_config = pd.read_csv(config_path, dtype=str)
        else:
            df_config = pd.read_excel(config_path, dtype=str)
    except Exception as e:
        raise ValueError("Could not read config '{}': {}".format(config_path.name, e))

    # Normalise headers
    df_config.columns = [c.strip().lower() for c in df_config.columns]

    # Validate required columns exist
    required_cols = {"customer_column", "wms_field"}
    missing_cols  = required_cols - set(df_config.columns)
    if missing_cols:
        raise ValueError(
            "Config '{}' is missing columns: {}. "
            "Required headers: customer_column, wms_field".format(
                config_path.name, missing_cols
            )
        )

    # Strip all values
    for col in df_config.columns:
        df_config[col] = df_config[col].astype(str).str.strip()

    # Read optional metadata from dedicated columns — first non-blank value wins
    def _first_value(col_name):
        if col_name not in df_config.columns:
            return None
        vals = df_config[col_name].replace({"nan": "", "None": "", "": None}).dropna()
        return vals.iloc[0] if not vals.empty else None

    customer_name = _first_value("customer_name") or customer_key.replace("_", " ").title()
    date_format   = _first_value("date_format")

    # Keep only the two mapping columns from here on
    df_config = df_config[["customer_column", "wms_field"]].copy()

    # Drop blank mapping rows
    df_config = df_config.dropna(subset=["customer_column", "wms_field"])
    df_config = df_config[
        (df_config["customer_column"] != "") &
        (df_config["wms_field"]       != "") &
        (df_config["customer_column"] != "nan") &
        (df_config["wms_field"]       != "nan")
    ]

    if df_config.empty:
        raise ValueError("Config '{}' has no valid mapping rows.".format(config_path.name))

    # Detect duplicate customer_column entries
    dupes = df_config[df_config["customer_column"].duplicated()]["customer_column"].tolist()
    if dupes:
        raise ValueError(
            "Config '{}' has duplicate customer_column entries: {}. "
            "Each source column must appear only once.".format(config_path.name, dupes)
        )

    # Validate all wms_field values are recognised WMS fields
    invalid_fields = [v for v in df_config["wms_field"].tolist() if v not in ALL_WMS_FIELDS]
    if invalid_fields:
        raise ValueError(
            "Config '{}' contains unrecognised WMS fields: {}. "
            "Valid fields: {}".format(config_path.name, invalid_fields, ALL_WMS_FIELDS)
        )

    column_map = dict(zip(df_config["customer_column"], df_config["wms_field"]))

    log.info("Loaded config: '{}' — {} mapping(s) for {}".format(
        config_path.name, len(column_map), customer_name
    ))
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
                        "Mandatory field '{}' is not mapped in this config.".format(field)
                    )
        except (FileNotFoundError, ValueError) as e:
            errors.append(str(e))
        reports[customer_key] = {"errors": errors, "warnings": warnings}
    return reports


# ─── STEP 2: READ EXCEL ──────────────────────────────────────────────────────

def read_excel(file_path, sheet_name=0) -> pd.DataFrame:
    """
    Read a customer Excel order file into a DataFrame.
    keep_default_na=False prevents values like NA/N/A being silently coerced to NaN.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError("File not found: {}".format(file_path))
    if file_path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError("Unsupported file type: '{}'".format(file_path.suffix))
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            dtype=str,
            keep_default_na=False,
        )
    except Exception as e:
        raise ValueError("Could not read Excel: {}".format(e))

    df.columns = [str(c).strip() for c in df.columns]
    df.replace("", pd.NA, inplace=True)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError("Excel file has no data rows.")

    log.info("Read {} rows. Columns: {}".format(len(df), list(df.columns)))
    return df


def is_valid_xlsx(file_bytes: bytes) -> bool:
    """
    Verify a file is a genuine XLSX or XLS by checking magic bytes.
    XLSX/ZIP: PK\\x03\\x04  |  XLS BIFF8: \\xd0\\xcf\\x11\\xe0
    """
    if len(file_bytes) < 4:
        return False
    return (
        file_bytes[:4] == b"PK\x03\x04" or
        file_bytes[:4] == b"\xd0\xcf\x11\xe0"
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
    column_map : dict   {customer_column: wms_field}
    case_insensitive_source : bool
        When True, incoming column names matched case-insensitively.
    """
    warnings   = []
    rename_map = {}

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
                "UNMAPPED: '{}' is not in the config and will be dropped. "
                "Add it to the config file if it is needed.".format(col)
            )

    df_mapped    = df.rename(columns=rename_map)
    cols_present = [c for c in ALL_WMS_FIELDS if c in df_mapped.columns]
    return df_mapped[cols_present], warnings


# ─── STEP 4: VALIDATE ────────────────────────────────────────────────────────

def validate(df: pd.DataFrame) -> tuple:
    """
    Validate mandatory fields and numeric fields using vectorised operations.
    """
    error_messages = []
    row_errors: dict = {}

    def _add_error(idx, msg):
        row_errors.setdefault(idx, []).append(msg)

    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            error_messages.append(
                "MISSING COLUMN: Mandatory field '{}' not found in mapped output.".format(field)
            )

    # Mandatory blank checks — vectorised
    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            continue
        blank_mask = (
            df[field].isna() |
            df[field].astype(str).str.strip().isin(["", "nan", "None"])
        )
        for idx in df.index[blank_mask]:
            _add_error(idx, "'{}' is blank".format(field))

    # Numeric field checks — vectorised
    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        present_mask = (
            df[field].notna() &
            ~df[field].astype(str).str.strip().isin(["", "nan", "None"])
        )
        if not present_mask.any():
            continue
        cleaned = (
            df.loc[present_mask, field]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        numeric_attempt = pd.to_numeric(cleaned, errors="coerce")
        for idx in numeric_attempt.index[numeric_attempt.isna()]:
            _add_error(idx, "'{}' non-numeric: '{}'".format(field, df.at[idx, field]))

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
            "{} row(s) failed validation (Excel rows: {})".format(
                len(row_errors), [i + 2 for i in error_indices]
            )
        )

    log.info("Validation: {} valid, {} error row(s).".format(len(df_valid), len(df_errors)))
    return df_valid, df_errors, error_messages


# ─── STEP 5: CLEAN DATA ──────────────────────────────────────────────────────

def clean_data(df: pd.DataFrame, date_format: str = None) -> pd.DataFrame:
    """
    Apply final type cleaning before export.
    """
    df = df.copy()

    for col in df.select_dtypes(include=["str", "object"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "None": "", "NaN": "", "<NA>": ""})
        )

    if "ORDER_DATE" in df.columns:
        if date_format:
            df["ORDER_DATE"] = pd.to_datetime(
                df["ORDER_DATE"], format=date_format, errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        else:
            df["ORDER_DATE"] = pd.to_datetime(
                df["ORDER_DATE"], dayfirst=True, errors="coerce"
            ).dt.strftime("%Y-%m-%d")

    for field in NUMERIC_FIELDS:
        if field not in df.columns:
            continue
        df[field] = pd.to_numeric(
            df[field].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

    if "ORIG_QTY_ORDERED" in df.columns:
        df["ORIG_QTY_ORDERED"] = df["ORIG_QTY_ORDERED"].apply(
            lambda x: int(x) if pd.notna(x) and x == int(x) else x
        )

    return df


# ─── STEP 6: EXPORT ──────────────────────────────────────────────────────────

def export_csv(df: pd.DataFrame, customer_key: str, output_dir: Path = OUTPUT_DIR) -> Path:
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / "wms_output_{}_{}.csv".format(customer_key, ts)
    cols     = [c for c in ALL_WMS_FIELDS if c in df.columns]
    df[cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info("Exported {} rows to '{}'".format(len(df), out_path))
    return out_path


def export_error_report(
    df_errors: pd.DataFrame,
    customer_key: str,
    output_dir: Path = OUTPUT_DIR,
) -> Path | None:
    if df_errors.empty:
        return None
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / "wms_errors_{}_{}.csv".format(customer_key, ts)
    df_errors.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.warning("Error report written to '{}'".format(out_path))
    return out_path


def cleanup_output_dir(output_dir: Path = OUTPUT_DIR, keep_days: int = 30) -> int:
    """Delete CSV files in output_dir older than keep_days days."""
    cutoff  = datetime.now().timestamp() - (keep_days * 86400)
    deleted = 0
    for f in output_dir.glob("*.csv"):
        if f.stat().st_mtime < cutoff:
            f.unlink()
            deleted += 1
    if deleted:
        log.info("Cleanup: {} file(s) removed from '{}'".format(deleted, output_dir))
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
            print("\nValid rows  : {}".format(len(df_clean)))
            print("Error rows  : {}".format(len(df_errors)))
            if warnings:
                print("\nWarnings:")
                for w in warnings:
                    print("  ! {}".format(w))
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
    parser.add_argument("--file",     "-f", required=False,
                        help="Path to the customer Excel file (.xlsx)")
    parser.add_argument("--customer", "-c", required=False,
                        help="Customer config key. Available: {}".format(list_customers()))
    parser.add_argument("--sheet",    "-s", default=0,
                        help="Sheet name or index (default: 0 = first sheet)")
    parser.add_argument("--yes",      "-y", action="store_true",
                        help="Skip manual review prompt and export immediately")
    parser.add_argument("--cleanup",  action="store_true",
                        help="Delete output files older than 30 days and exit")
    args = parser.parse_args()

    if args.cleanup:
        n = cleanup_output_dir()
        print("Cleanup: {} file(s) removed.".format(n))
        return

    if not args.file or not args.customer:
        parser.error("--file and --customer are required unless --cleanup is used")

    result = run_conversion(args.file, args.customer, args.sheet, args.yes)
    print("\n" + "=" * 60)
    print("RESULT:", "SUCCESS" if result["success"] else "FAILED")
    print("Valid rows : {}".format(result["valid_rows"]))
    print("Error rows : {}".format(result["error_rows"]))
    if result["output_path"]: print("Output     : {}".format(result["output_path"]))
    if result["error_path"]:  print("Errors     : {}".format(result["error_path"]))
    for e in result["errors"]:   print("  x {}".format(e))
    for w in result["warnings"]: print("  ! {}".format(w))


if __name__ == "__main__":
    main()
