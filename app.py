"""
app.py
======
WMS Order File Converter — Streamlit Web Interface
---------------------------------------------------
Provides a browser-based UI for uploading customer Excel files,
selecting the customer mapping, reviewing the mapped output,
and downloading the final WMS-ready CSV.

Run with:
    streamlit run app.py

Dependencies:
    - streamlit   : web UI framework
    - pandas      : dataframe display and manipulation
    - converter   : local core engine module (converter.py)
"""

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
    ALL_WMS_FIELDS,
    MANDATORY_FIELDS,
    MAPPINGS_DIR,
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

def get_mappings_fingerprint():
    config_files = sorted(
        p for ext in ("*.csv", "*.xlsx")
        for p in MAPPINGS_DIR.glob(ext)
        if p.stem != "template"
    )
    fingerprint_parts = []
    for file_path in config_files:
        stat = file_path.stat()
        fingerprint_parts.append(f"{file_path.name}:{stat.st_mtime_ns}:{stat.st_size}")
    return "|".join(fingerprint_parts)

@st.cache_data(show_spinner=False)
def cached_list_customers(mappings_fingerprint):
    _ = mappings_fingerprint
    return list_customers()

@st.cache_data(show_spinner=False)
def cached_load_customer_config(customer_key, mappings_fingerprint):
    _ = mappings_fingerprint
    return load_customer_config(customer_key)

@st.cache_data(show_spinner=False)
def cached_validate_all_configs(mappings_fingerprint):
    _ = mappings_fingerprint
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
            "The filename becomes the customer key (e.g. acme_corp.csv → acme_corp)."
        ),
    )

    if uploaded_config is not None:
        config_dest = MAPPINGS_DIR / uploaded_config.name
        if config_dest.stem == "template":
            st.error("Cannot overwrite the template file. Rename your config and re-upload.")
        elif config_dest.suffix.lower() not in (".csv", ".xlsx"):
            st.error("Only .csv and .xlsx config files are accepted.")
        else:
            config_dest.write_bytes(uploaded_config.getvalue())
            st.success(
                f"Config saved: **{uploaded_config.name}**. "
                f"Customer key: **{config_dest.stem}**"
            )

    st.divider()

    mappings_fingerprint = get_mappings_fingerprint()

    customers = cached_list_customers(mappings_fingerprint)
    if not customers:
        st.error("No customer configs found in mappings/ directory.")
        st.stop()

    all_config_reports = cached_validate_all_configs(mappings_fingerprint)
    invalid_configs = sorted([key for key, report in all_config_reports.items() if report["errors"]])
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
        format_func=lambda key: f"{key} {'(invalid)' if key in invalid_configs else ''}"
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

    if config and "description" in config:
        st.caption(config["description"])

    st.subheader("Config health")
    if selected_report["warnings"]:
        for warn in selected_report["warnings"]:
            st.warning(warn)
    elif config_is_valid:
        st.success("No config warnings detected.")

    st.divider()
    st.subheader("Column Mapping Preview")
    if config:
        mapping_df = pd.DataFrame(
            list(config["column_map"].items()),
            columns=["Customer column", "WMS field"]
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
    help="The customer's raw order file. Column names do not need to match — "
         "the mapping config handles translation.",
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

    # Save uploaded file to a temp location so converter can read it
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(uploaded_file.read())
        tmp_path = Path(tmp.name)

    try:
        with st.spinner("Reading and mapping file..."):
            try:
                df_raw              = read_excel(tmp_path, sheet_name)
                df_mapped, warnings = apply_mapping(
                    df_raw, config["column_map"], case_insensitive_source=True
                )
                df_valid, df_errors, val_errors = validate(df_mapped)
                df_clean            = clean_data(df_valid)
            except (FileNotFoundError, ValueError) as e:
                st.error(str(e))
                st.stop()

        # ── Warnings ────────────────────────────────────────────────────────────
        if warnings:
            with st.expander(f"⚠ {len(warnings)} mapping warning(s)", expanded=True):
                for w in warnings:
                    st.warning(w)

        # ── Validation errors ───────────────────────────────────────────────────
        col1, col2, col3 = st.columns(3)
        col1.metric("Total rows read",  len(df_raw))
        col2.metric("Valid rows",        len(df_valid),  delta=None)
        col3.metric("Error rows",        len(df_errors),
                    delta=f"-{len(df_errors)}" if len(df_errors) > 0 else None,
                    delta_color="inverse")

        if val_errors:
            with st.expander(f"✗ {len(val_errors)} validation issue(s)", expanded=True):
                for e in val_errors:
                    st.error(e)

        # ── Raw input preview ───────────────────────────────────────────────────
        st.subheader("3. Review")

        tab1, tab2, tab3 = st.tabs(["Mapped output", "Error rows", "Raw input"])

        with tab1:
            if df_clean.empty:
                st.warning("No valid rows to display.")
            else:
                # Highlight mandatory fields
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

        # ── Export ───────────────────────────────────────────────────────────────
        st.subheader("4. Export")

        if df_clean.empty:
            st.error("Nothing to export — all rows have validation errors.")
            st.session_state["export_payload"] = None
        else:
            cols_ordered = [c for c in ALL_WMS_FIELDS if c in df_clean.columns]
            csv_bytes = df_clean[cols_ordered].to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

            st.info(
                f"Ready to export **{len(df_clean)} valid rows** as WMS CSV. "
                + (f"**{len(df_errors)} error rows** will be excluded." if len(df_errors) > 0 else "")
            )

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_fname = f"wms_output_{customer_key}_{ts}.csv"
            error_fname = f"wms_errors_{customer_key}_{ts}.csv"
            err_csv = df_errors.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig") if not df_errors.empty else None

            # Keep export bytes in session so downloads remain available after reruns in deployed Streamlit.
            st.session_state["export_payload"] = {
                "output_fname": output_fname,
                "csv_bytes": csv_bytes,
                "error_fname": error_fname,
                "err_csv": err_csv,
            }
    finally:
        tmp_path.unlink(missing_ok=True)

else:
    if config_is_valid:
        st.info("Upload a customer Excel file to begin.")
    else:
        st.info("Fix config errors in mappings first, then upload a file.")

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
    "WMS Order File Converter | WMS Department | "
    "To add a new customer, copy mappings/template.csv and fill in customer_column / wms_field."
)
