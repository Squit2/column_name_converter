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

MAPPINGS_DIR = Path(__file__).parent / "mappings"
OUTPUT_DIR   = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ─── STEP 1: LOAD CONFIG ─────────────────────────────────────────────────────

def load_customer_config(customer_key):
    """
    Load a customer mapping config from the mappings/ directory.
    Accepts .csv or .xlsx files with exactly two columns:
        customer_column  — the column name as it appears in the customer's file
        wms_field        — the WMS standard field it maps to

    Example config (gigly_gulp.csv):
        customer_column,wms_field
        DocNo,ORDER_REF
        DocDate,ORDER_DATE
        DebtorCode,CUST_CODE
        ...

    Parameters
    ----------
    customer_key : str
        Filename of the config without extension, e.g. "gigly_gulp"

    Returns
    -------
    dict with keys:
        customer_name : str   — derived from the config filename
        column_map    : dict  — {customer_column: wms_field}
    """
    # Search for a matching .csv or .xlsx config file
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
            f"Create a new config as mappings/{customer_key}.csv with columns: "
            f"customer_column, wms_field"
        )

    # Read the config file into a dataframe
    try:
        if config_path.suffix.lower() == ".csv":
            df_config = pd.read_csv(config_path, dtype=str)
        else:
            df_config = pd.read_excel(config_path, dtype=str)
    except Exception as e:
        raise ValueError(f"Could not read config file '{config_path.name}': {e}")

    # Normalise column headers — strip whitespace and lowercase for robustness
    df_config.columns = [c.strip().lower() for c in df_config.columns]

    # Validate required columns exist
    required_cols = {"customer_column", "wms_field"}
    missing_cols  = required_cols - set(df_config.columns)
    if missing_cols:
        raise ValueError(
            f"Config '{config_path.name}' is missing columns: {missing_cols}.\n"
            f"Config must have exactly these column headers: customer_column, wms_field"
        )

    # Drop any rows where either column is blank
    df_config = df_config.dropna(subset=["customer_column", "wms_field"])
    df_config["customer_column"] = df_config["customer_column"].str.strip()
    df_config["wms_field"]       = df_config["wms_field"].str.strip()
    df_config = df_config[
        (df_config["customer_column"] != "") &
        (df_config["wms_field"]       != "")
    ]

    if df_config.empty:
        raise ValueError(
            f"Config '{config_path.name}' has no valid mapping rows."
        )

    # Validate all wms_field values are recognised WMS fields
    invalid_fields = [
        row["wms_field"] for _, row in df_config.iterrows()
        if row["wms_field"] not in ALL_WMS_FIELDS
    ]
    if invalid_fields:
        raise ValueError(
            f"Config '{config_path.name}' contains unrecognised WMS fields: "
            f"{invalid_fields}.\nValid WMS fields are: {ALL_WMS_FIELDS}"
        )

    # Build the column_map dict from the two columns
    column_map    = dict(zip(df_config["customer_column"], df_config["wms_field"]))
    customer_name = customer_key.replace("_", " ").title()

    log.info(
        f"Loaded config: '{config_path.name}' — "
        f"{len(column_map)} column mapping(s) for {customer_name}"
    )
    return {
        "customer_name": customer_name,
        "column_map":    column_map,
    }


def list_customers():
    """Return all available customer keys (config filenames without extension)."""
    keys = set()
    for ext in ("*.csv", "*.xlsx"):
        for p in MAPPINGS_DIR.glob(ext):
            if p.stem != "template":
                keys.add(p.stem)
    return sorted(keys)


def validate_all_customer_configs():
    """
    Attempt to load every customer config in the mappings/ directory
    and return a report of errors and warnings for each.

    Returns
    -------
    dict of {customer_key: {"errors": [...], "warnings": [...]}}
    """
    reports = {}
    for customer_key in list_customers():
        errors   = []
        warnings = []
        try:
            config = load_customer_config(customer_key)
            column_map = config["column_map"]

            # Warn if any mandatory WMS fields have no mapping
            mapped_wms_fields = set(column_map.values())
            for field in MANDATORY_FIELDS:
                if field not in mapped_wms_fields:
                    warnings.append(
                        f"Mandatory field '{field}' is not mapped in this config."
                    )
        except (FileNotFoundError, ValueError) as e:
            errors.append(str(e))

        reports[customer_key] = {"errors": errors, "warnings": warnings}
    return reports


# ─── STEP 2: READ EXCEL ──────────────────────────────────────────────────────

def read_excel(file_path, sheet_name=0):
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if file_path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError(f"Unsupported file type: '{file_path.suffix}'")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        raise ValueError(f"Could not read Excel: {e}")
    df.columns = [str(c).strip() for c in df.columns]
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    if df.empty:
        raise ValueError("Excel file has no data rows.")
    log.info(f"Read {len(df)} rows. Columns: {list(df.columns)}")
    return df


# ─── STEP 3: APPLY MAPPING ───────────────────────────────────────────────────

def apply_mapping(df, column_map, case_insensitive_source=False):
    """
    Rename DataFrame columns from customer names to WMS standard names.

    Parameters
    ----------
    df : pd.DataFrame
        Raw customer dataframe.
    column_map : dict
        {customer_column: wms_field} from the config.
    case_insensitive_source : bool
        If True, incoming column names are matched to the config
        case-insensitively. Useful when a customer file has inconsistent
        casing (e.g. "docno" vs "DocNo"). Defaults to False.
    """
    warnings = []
    rename_map = {}

    if case_insensitive_source:
        # Build a lowercased lookup so "docno" matches "DocNo" in config
        lower_map = {k.lower(): v for k, v in column_map.items()}
        for col in df.columns:
            wms_field = lower_map.get(col.lower())
            if wms_field:
                rename_map[col] = wms_field
            else:
                warnings.append(
                    f"UNMAPPED: '{col}' is not in the config and will be dropped. "
                    f"Add it to the config file if it is needed."
                )
    else:
        for col in df.columns:
            if col in column_map:
                rename_map[col] = column_map[col]
            else:
                warnings.append(
                    f"UNMAPPED: '{col}' is not in the config and will be dropped. "
                    f"Add it to the config file if it is needed."
                )

    df_mapped = df.rename(columns=rename_map)
    cols_present = [c for c in ALL_WMS_FIELDS if c in df_mapped.columns]
    return df_mapped[cols_present], warnings


# ─── STEP 4: VALIDATE ────────────────────────────────────────────────────────

def validate(df):
    error_messages = []
    row_errors = {}

    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            error_messages.append(f"MISSING COLUMN: Mandatory field '{field}' not found.")

    for idx, row in df.iterrows():
        issues = []
        for field in MANDATORY_FIELDS:
            if field not in df.columns:
                continue
            val = row.get(field, None)
            if pd.isna(val) or str(val).strip() in ("", "nan"):
                issues.append(f"'{field}' is blank")
        for field in ("ORIG_QTY_ORDERED", "AOPT_DEC_FIELD3", "AOPT_DEC_FIELD4"):
            if field not in df.columns:
                continue
            val = row.get(field, None)
            if pd.notna(val) and str(val).strip() not in ("", "nan"):
                try:
                    float(str(val).replace(",", ""))
                except ValueError:
                    issues.append(f"'{field}' non-numeric: '{val}'")
        if issues:
            row_errors[idx] = issues

    error_indices = list(row_errors.keys())
    valid_indices = [i for i in df.index if i not in error_indices]
    df_valid  = df.loc[valid_indices].copy()
    df_errors = df.loc[error_indices].copy() if error_indices else pd.DataFrame()
    if not df_errors.empty:
        df_errors = df_errors.copy()
        df_errors["VALIDATION_ERRORS"] = df_errors.index.map(
            lambda i: " | ".join(row_errors[i])
        )

    if row_errors:
        error_messages.append(
            f"{len(row_errors)} row(s) failed validation (Excel rows: "
            f"{[i+2 for i in error_indices]})"
        )
    log.info(f"Validation: {len(df_valid)} valid, {len(df_errors)} errors.")
    return df_valid, df_errors, error_messages


# ─── STEP 5: CLEAN DATA ──────────────────────────────────────────────────────

def clean_data(df):
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace("nan", "")
    if "ORDER_DATE" in df.columns:
        df["ORDER_DATE"] = pd.to_datetime(
            df["ORDER_DATE"], dayfirst=True, errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    for field in ("ORIG_QTY_ORDERED", "AOPT_DEC_FIELD3", "AOPT_DEC_FIELD4"):
        if field not in df.columns:
            continue
        df[field] = pd.to_numeric(
            df[field].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )
    if "ORIG_QTY_ORDERED" in df.columns:
        df["ORIG_QTY_ORDERED"] = df["ORIG_QTY_ORDERED"].apply(
            lambda x: int(x) if pd.notna(x) and x == int(x) else x
        )
    return df


# ─── STEP 6: EXPORT ──────────────────────────────────────────────────────────

def export_csv(df, customer_key, output_dir=OUTPUT_DIR):
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{customer_key}_{ts}.csv"
    out_path = output_dir / filename
    cols     = [c for c in ALL_WMS_FIELDS if c in df.columns]
    df[cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"Exported {len(df)} rows to '{out_path}'")
    return out_path


def export_error_report(df_errors, customer_key, output_dir=OUTPUT_DIR):
    if df_errors.empty:
        return None
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"wms_errors_{customer_key}_{ts}.csv"
    out_path = output_dir / filename
    df_errors.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.warning(f"Error report: '{out_path}'")
    return out_path


# ─── MAIN PIPELINE ───────────────────────────────────────────────────────────

def run_conversion(file_path, customer_key, sheet_name=0, auto_confirm=False, output_dir=OUTPUT_DIR):
    result = {"success": False, "output_path": None, "error_path": None,
              "valid_rows": 0, "error_rows": 0, "warnings": [], "errors": []}
    try:
        config               = load_customer_config(customer_key)
        df_raw               = read_excel(file_path, sheet_name)
        df_mapped, warnings  = apply_mapping(df_raw, config["column_map"])
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

        df_clean = clean_data(df_valid)

        if not auto_confirm:
            print("\n" + "="*60)
            print("PREVIEW — First 5 rows:")
            print("="*60)
            print(df_clean.head(5).to_string(index=False))
            print(f"\nValid rows  : {len(df_clean)}")
            print(f"Error rows  : {len(df_errors)}")
            if warnings:
                print(f"\nWarnings:")
                for w in warnings: print(f"  ! {w}")
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
    args = parser.parse_args()

    result = run_conversion(args.file, args.customer, args.sheet, args.yes)
    print("\n" + "="*60)
    print("RESULT:", "SUCCESS" if result["success"] else "FAILED")
    print(f"Valid rows : {result['valid_rows']}")
    print(f"Error rows : {result['error_rows']}")
    if result["output_path"]: print(f"Output     : {result['output_path']}")
    if result["error_path"]:  print(f"Errors     : {result['error_path']}")
    for e in result["errors"]:   print(f"  x {e}")
    for w in result["warnings"]: print(f"  ! {w}")

if __name__ == "__main__":
    main()
