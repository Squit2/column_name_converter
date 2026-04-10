"""
app.py
======
WMS Order File Converter — Streamlit Web Interface

Changes from review:
  - Config upload: path traversal sanitisation on filename
  - Config upload: 1 MB file size guard
  - Order file upload: magic byte validation (XLSX/XLS)
  - Temp file cleanup made robust (unconditional unlink, no reliance on finally)
  - Footer updated
"""

import re
import tempfile
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

from converter import (
    list_customers,
    load_customer_config,
    validate_all_customer_configs,
    read_excel,
    apply_mapping,
    validate,
    clean_data,
    is_valid_xlsx,
    ALL_WMS_FIELDS,
    MANDATORY_FIELDS,
    MAPPINGS_DIR,
    MAX_CONFIG_BYTES,
)

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="WMS Order Converter",
    page_icon="📦",
    layout="wide"
)

st.title("📦 WMS Order File Converter")
st.caption("Convert customer Excel order files into WMS-ready CSV format.")

if "export_payload" not in st.session_state:
    st.session_state["export_payload"] = None

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def get_mappings_fingerprint() -> str:
    """
    Build a cache-busting string from every config file's name, size and
    modification time. Changes whenever a config is added, removed or updated.
    """
    config_files = sorted(
        p for ext in ("*.csv", "*.xlsx")
        for p in MAPPINGS_DIR.glob(ext)
        if p.stem != "template"
    )
    parts = []
    for p in config_files:
        stat = p.stat()
        parts.append(f"{p.name}:{stat.st_mtime_ns}:{stat.st_size}")
    return "|".join(parts)


def sanitise_config_filename(raw_name: str) -> str:
    """
    Sanitise an uploaded config filename to prevent path traversal.
    Keeps only alphanumerics, hyphens and underscores in the stem;
    forces the extension to lowercase .csv or .xlsx.

    e.g. "../../converter.py"  → "_____converter_py.py"  → rejected (bad ext)
         "Gigly Gulp 2026.csv" → "Gigly_Gulp_2026.csv"
    """
    p    = Path(raw_name)
    stem = re.sub(r"[^\w\-]", "_", p.stem)          # replace anything unsafe
    ext  = p.suffix.lower()
    return f"{stem}{ext}"


@st.cache_data(show_spinner=False)
def cached_list_customers(fingerprint: str):
    _ = fingerprint
    return list_customers()


@st.cache_data(show_spinner=False)
def cached_load_customer_config(customer_key: str, fingerprint: str):
    _ = fingerprint
    return load_customer_config(customer_key)


@st.cache_data(show_spinner=False)
def cached_validate_all_configs(fingerprint: str):
    _ = fingerprint
    return validate_all_customer_configs()


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")

    # ── Upload a new config file ─────────────────────────────────────────────
    st.subheader("Upload config")
    uploaded_config = st.file_uploader(
        "Drop a customer config here (.csv or .xlsx)",
        type=["csv", "xlsx"],
        help=(
            "Config must have two columns: customer_column and wms_field. "
            "The filename becomes the customer key (e.g. acme_corp.csv → acme_corp). "
            "Maximum file size: 1 MB."
        ),
    )

    if uploaded_config is not None:
        config_bytes = uploaded_config.getvalue()

        # Guard 1: file size
        if len(config_bytes) > MAX_CONFIG_BYTES:
            st.error(
                f"Config file is too large "
                f"({len(config_bytes) / 1024:.0f} KB). Maximum allowed is 1 MB."
            )
        else:
            # Guard 2: sanitise filename (path traversal protection)
            safe_name   = sanitise_config_filename(uploaded_config.name)
            config_dest = MAPPINGS_DIR / safe_name

            if config_dest.stem == "template":
                st.error("Cannot overwrite the template file. Rename your config and re-upload.")
            elif config_dest.suffix.lower() not in (".csv", ".xlsx"):
                st.error("Only .csv and .xlsx config files are accepted.")
            else:
                config_dest.write_bytes(config_bytes)
                st.success(
                    f"Config saved: **{safe_name}**. "
                    f"Customer key: **{config_dest.stem}**"
                )
                if safe_name != uploaded_config.name:
                    st.info(
                        f"Filename was sanitised: "
                        f"'{uploaded_config.name}' → '{safe_name}'"
                    )

    st.divider()

    # Fingerprint is computed after any upload so new files are visible immediately
    mappings_fingerprint = get_mappings_fingerprint()

    customers = cached_list_customers(mappings_fingerprint)
    if not customers:
        st.error("No customer configs found in mappings/ directory.")
        st.stop()

    all_config_reports = cached_validate_all_configs(mappings_fingerprint)
    invalid_configs    = sorted(
        key for key, r in all_config_reports.items() if r["errors"]
    )
    if invalid_configs:
        st.warning(f"{len(invalid_configs)} invalid config(s) detected.")

    if st.button("Validate all configs"):
        for key in customers:
            report = all_config_reports.get(key, {"errors": [], "warnings": []})
            if report["errors"]:
                st.error(f"{key}: {' | '.join(report['errors'])}")
            elif report["warnings"]:
                st.warning(f"{key}: {' | '.join(report['warnings'])}")
            else:
                st.success(f"{key}: OK")

    customer_key = st.selectbox(
        "Customer",
        options=customers,
        help="Select which customer's mapping config to use.",
        format_func=lambda k: f"{k} {'(invalid)' if k in invalid_configs else ''}",
    )

    selected_report = all_config_reports.get(customer_key, {"errors": [], "warnings": []})
    config_is_valid = len(selected_report["errors"]) == 0

    if config_is_valid:
        config = cached_load_customer_config(customer_key, mappings_fingerprint)
        st.success(f"Config loaded: **{config['customer_name']}**")
    else:
        config = None
        st.error("Selected config is invalid and cannot be used.")
        for err in selected_report["errors"]:
            st.error(err)

    st.subheader("Config health")
    if selected_report["warnings"]:
        for warn in selected_report["warnings"]:
            st.warning(warn)
    elif config_is_valid:
        st.success("No config warnings detected.")

    st.divider()
    st.subheader("Column mapping preview")
    if config:
        mapping_df = pd.DataFrame(
            list(config["column_map"].items()),
            columns=["Customer column", "WMS field"],
        )
        mapping_df["Mandatory"] = mapping_df["WMS field"].apply(
            lambda f: "Yes" if f in MANDATORY_FIELDS else ""
        )
        st.dataframe(mapping_df, hide_index=True, use_container_width=True)
    else:
        st.info("Mapping preview unavailable for invalid config.")

# ─── MAIN: FILE UPLOAD ───────────────────────────────────────────────────────

st.subheader("1. Upload customer order file")

uploaded_file = st.file_uploader(
    "Upload Excel file (.xlsx or .xls)",
    type=["xlsx", "xls"],
    help=(
        "The customer's raw order file. Column names do not need to match — "
        "the mapping config handles translation."
    ),
    disabled=not config_is_valid,
)

sheet_input = st.text_input(
    "Sheet name or index",
    value="0",
    help="Enter 0 for the first sheet, 1 for second, or type the exact sheet name.",
    disabled=not config_is_valid,
)

try:
    sheet_name = int(sheet_input)
except ValueError:
    sheet_name = sheet_input

# ─── MAIN: PROCESS ───────────────────────────────────────────────────────────

if uploaded_file:
    st.subheader("2. Processing")

    file_bytes = uploaded_file.read()

    # Magic byte validation — confirm the file is actually XLSX/XLS before
    # passing to pandas. Extension alone is client-side and easily spoofed.
    if not is_valid_xlsx(file_bytes):
        st.error(
            "The uploaded file does not appear to be a valid Excel file. "
            "Please upload a genuine .xlsx or .xls file."
        )
        st.stop()

    # Write to a temp file; use the uploaded file's actual extension so
    # pandas picks the right engine
    suffix   = Path(uploaded_file.name).suffix or ".xlsx"
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(file_bytes)
            tmp_path = Path(tmp.name)

        with st.spinner("Reading and mapping file..."):
            try:
                df_raw              = read_excel(tmp_path, sheet_name)
                df_mapped, warnings = apply_mapping(
                    df_raw, config["column_map"], case_insensitive_source=True
                )
                df_valid, df_errors, val_errors = validate(df_mapped)
                df_clean            = clean_data(
                    df_valid, date_format=config.get("date_format")
                )
            except (FileNotFoundError, ValueError) as e:
                st.error(str(e))
                st.stop()

    finally:
        # Always clean up the temp file, even if st.stop() was called above
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    # ── Warnings ──────────────────────────────────────────────────────────────
    if warnings:
        with st.expander(f"⚠ {len(warnings)} mapping warning(s)", expanded=True):
            for w in warnings:
                st.warning(w)

    # ── Metrics ───────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    col1.metric("Total rows read", len(df_raw))
    col2.metric("Valid rows",       len(df_valid))
    col3.metric(
        "Error rows",
        len(df_errors),
        delta=f"-{len(df_errors)}" if len(df_errors) > 0 else None,
        delta_color="inverse",
    )

    if val_errors:
        with st.expander(f"✗ {len(val_errors)} validation issue(s)", expanded=True):
            for e in val_errors:
                st.error(e)

    # ── Review tabs ───────────────────────────────────────────────────────────
    st.subheader("3. Review")
    tab1, tab2, tab3 = st.tabs(["Mapped output", "Error rows", "Raw input"])

    with tab1:
        if df_clean.empty:
            st.warning("No valid rows to display.")
        else:
            mandatory_in_df = [f for f in MANDATORY_FIELDS if f in df_clean.columns]
            st.caption(
                f"Showing {len(df_clean)} valid rows. "
                f"Mandatory fields: {', '.join(mandatory_in_df)}"
            )
            st.dataframe(df_clean, use_container_width=True, hide_index=True)

    with tab2:
        if df_errors.empty:
            st.success("No error rows.")
        else:
            st.caption(
                f"{len(df_errors)} row(s) failed validation. "
                "Fix the source file and re-upload."
            )
            st.dataframe(df_errors, use_container_width=True, hide_index=True)

    with tab3:
        st.caption("Original columns as received from customer.")
        st.dataframe(df_raw, use_container_width=True, hide_index=True)

    # ── Export ────────────────────────────────────────────────────────────────
    st.subheader("4. Export")

    if df_clean.empty:
        st.error("Nothing to export — all rows have validation errors.")
        st.session_state["export_payload"] = None
    else:
        cols_ordered = [c for c in ALL_WMS_FIELDS if c in df_clean.columns]
        csv_bytes    = (
            df_clean[cols_ordered]
            .to_csv(index=False, encoding="utf-8-sig")
            .encode("utf-8-sig")
        )
        err_csv = (
            df_errors.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            if not df_errors.empty else None
        )

        st.info(
            f"Ready to export **{len(df_clean)} valid rows** as WMS CSV."
            + (f" **{len(df_errors)} error row(s)** will be excluded."
               if len(df_errors) > 0 else "")
        )

        ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_fname = f"wms_output_{customer_key}_{ts}.csv"
        error_fname  = f"wms_errors_{customer_key}_{ts}.csv"

        # Store in session state so download buttons survive Streamlit reruns
        st.session_state["export_payload"] = {
            "output_fname": output_fname,
            "csv_bytes":    csv_bytes,
            "error_fname":  error_fname,
            "err_csv":      err_csv,
        }

else:
    if config_is_valid:
        st.info("Upload a customer Excel file to begin.")
    else:
        st.info("Fix config errors in mappings first, then upload a file.")

# Download buttons live outside the if/else so they persist after rerun
payload = st.session_state.get("export_payload")
if payload:
    st.download_button(
        label="⬇ Download WMS CSV",
        data=payload["csv_bytes"],
        file_name=payload["output_fname"],
        mime="text/csv",
        type="primary",
    )
    if payload["err_csv"] is not None:
        st.download_button(
            label="⬇ Download error report",
            data=payload["err_csv"],
            file_name=payload["error_fname"],
            mime="text/csv",
        )

# ─── FOOTER ──────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "WMS Order File Converter | WMS / GIT Department | "
    "To add a new customer, copy mappings/template.csv and fill in "
    "customer_column / wms_field."
)
