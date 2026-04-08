"""
converter.py
============
WMS Order File Converter - Core Engine
"""

import json
import logging
import argparse
from collections import Counter
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

def _normalize_col_name(name):
    return str(name).strip().lower()

def validate_customer_config(config, config_name="<inline_config>"):
    errors = []
    warnings = []

    if not isinstance(config, dict):
        return {"errors": [f"Config '{config_name}' must be a JSON object."], "warnings": []}

    required = {"customer_name", "column_map"}
    missing = required - set(config.keys())
    if missing:
        errors.append(f"Config '{config_name}' missing keys: {sorted(missing)}")

    column_map = config.get("column_map")
    if not isinstance(column_map, dict) or not column_map:
        errors.append(f"Config '{config_name}' must define non-empty 'column_map'.")
        return {"errors": errors, "warnings": warnings}

    invalid_targets = sorted({target for target in column_map.values() if target not in ALL_WMS_FIELDS})
    if invalid_targets:
        errors.append(
            f"Config '{config_name}' has invalid WMS target field(s): {invalid_targets}"
        )

    counts = Counter(column_map.values())
    duplicate_targets = sorted([target for target, count in counts.items() if count > 1])
    if duplicate_targets:
        warnings.append(
            f"Config '{config_name}' maps multiple source columns to the same WMS field(s): {duplicate_targets}"
        )

    mapped_targets = set(column_map.values())
    missing_mandatory = [field for field in MANDATORY_FIELDS if field not in mapped_targets]
    if missing_mandatory:
        warnings.append(
            f"Config '{config_name}' does not map mandatory WMS field(s): {missing_mandatory}"
        )

    normalized_sources = [_normalize_col_name(src) for src in column_map.keys()]
    duplicate_sources = sorted(
        [source for source, count in Counter(normalized_sources).items() if count > 1]
    )
    if duplicate_sources:
        warnings.append(
            f"Config '{config_name}' has duplicate source column names after normalization: {duplicate_sources}"
        )

    return {"errors": errors, "warnings": warnings}

def load_customer_config(customer_key):
    config_path = MAPPINGS_DIR / f"{customer_key}.json"
    if not config_path.exists():
        available = [p.stem for p in MAPPINGS_DIR.glob("*.json") if p.stem != "template"]
        raise FileNotFoundError(
            f"No config found for '{customer_key}'.\nAvailable: {available}\n"
            f"Add a new customer by copying mappings/template.json"
        )
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    report = validate_customer_config(config, config_path.name)
    if report["errors"]:
        raise ValueError("\n".join(report["errors"]))
    config["_config_warnings"] = report["warnings"]
    log.info(f"Loaded config: {config['customer_name']}")
    return config

def list_customers():
    return [p.stem for p in MAPPINGS_DIR.glob("*.json") if p.stem != "template"]

def validate_all_customer_configs():
    reports = {}
    for customer_key in list_customers():
        config_path = MAPPINGS_DIR / f"{customer_key}.json"
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            reports[customer_key] = validate_customer_config(config, config_path.name)
        except json.JSONDecodeError as e:
            reports[customer_key] = {"errors": [f"Invalid JSON in '{config_path.name}': {e}"], "warnings": []}
        except Exception as e:
            reports[customer_key] = {"errors": [str(e)], "warnings": []}
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
def apply_mapping(df, column_map, case_insensitive_source=True):
    warnings = []
    rename_map = {}
    normalized_source_map = {}
    if case_insensitive_source:
        normalized_source_map = {
            _normalize_col_name(src): target for src, target in column_map.items()
        }

    for col in df.columns:
        if col in column_map:
            rename_map[col] = column_map[col]
        elif case_insensitive_source and _normalize_col_name(col) in normalized_source_map:
            target = normalized_source_map[_normalize_col_name(col)]
            warnings.append(
                f"CASE-NORMALIZED MATCH: '{col}' mapped to '{target}' via normalized source-column name."
            )
            rename_map[col] = target
        else:
            warnings.append(f"UNMAPPED: '{col}' could not be mapped and will be dropped.")
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
        df_errors["VALIDATION_ERRORS"] = df_errors.index.map(lambda i: " | ".join(row_errors[i]))

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
    for col in df.select_dtypes(include=["object", "string"]).columns:
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
    filename = f"wms_output_{customer_key}_{ts}.csv"
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
    parser.add_argument("--file",     "-f", required=True)
    parser.add_argument("--customer", "-c", required=True)
    parser.add_argument("--sheet",    "-s", default=0)
    parser.add_argument("--yes",      "-y", action="store_true")
    args = parser.parse_args()

    # Support both sheet index ("0", "1", ...) and sheet name.
    sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
    result = run_conversion(args.file, args.customer, sheet, args.yes)
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
