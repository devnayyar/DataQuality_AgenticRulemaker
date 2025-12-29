# hitl/app.py - DQ Rule Approval UI
import sys
import streamlit as st
import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Any

# --- Page Configuration ---
st.set_page_config(
    page_title="DQ Rule Approval",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Path Setup ---
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

# --- Imports ---
from hitl.controller import (create_review, submit_review, _load_reviews)
from llm.rule_validator import validate_rules
import os
import time

# --- Initialize Session State FIRST (before any usage) ---
if "sid" not in st.session_state:
    st.session_state.sid = None
if "auto_refresh_counter" not in st.session_state:
    st.session_state.auto_refresh_counter = 0
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

# --- Custom Styling ---
st.markdown("""
<style>
    .success-text { color: #09ab3b; font-weight: bold; }
    .warning-text { color: #ff2b2b; font-weight: bold; }
    .info-text { color: #0668bc; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

st.title("📊 Data Quality Rule Approval System")
st.markdown("Review, preview, and approve data quality & PII transformation rules before execution")
st.markdown("---")

# --- DEBUG INFO (Hidden by default) ---
with st.expander("🔍 Debug Info", expanded=False):
    st.write("Current Session State:")
    st.json({
        "sid": st.session_state.sid,
        "pending_reviews_file_exists": os.path.exists("pending_reviews.json"),
        "pending_reviews_count": len(_load_reviews()),
        "pending_sessions": list(_load_reviews().keys())
    })
    if st.button("🔄 Refresh All Sessions"):
        st.rerun()

# --- HELPER FUNCTION: Load CSV files ---
@st.cache_data(ttl=30)
def load_csv_file(path):
    """Load CSV safely with error handling"""
    try:
        if os.path.exists(path):
            return pd.read_csv(path)
        return None
    except Exception as e:
        st.error(f"Error loading {path}: {e}")
        return None

# --- SECTION 1: Select a Pending Review Session ---
if st.session_state.sid is None:
    st.header("Step 1️⃣: Select Review Session & View Results")
    
    _pending_reviews = _load_reviews()
    
    # Show ALL sessions, not just "pending"
    all_sessions = list(_pending_reviews.keys())
    
    if not all_sessions:
        st.info("✅ No sessions. Run `python jobs/batch_runner.py` to start processing.")
    else:
        # Create session selector with status indicator
        session_display_options = {}
        for sid in all_sessions:
            sess = _pending_reviews[sid]
            table_name = sess.get('table', 'Unknown')
            status = sess.get('status', 'unknown')
            status_emoji = {
                'pending': '⏳',
                'approved': '✅',
                'completed': '🎉',
                'rejected': '❌'
            }.get(status, '❓')
            
            display_text = f"{status_emoji} {table_name} ({status})"
            session_display_options[display_text] = sid
        
        selected_display = st.selectbox(
            "📋 Select table to view:",
            list(session_display_options.keys()),
            key="table_selector"
        )
        
        if st.button(f"▶️ Load & Review", use_container_width=True):
            st.session_state.sid = session_display_options[selected_display]
            st.rerun()

# --- SECTION 2: Review Session ---
else:
    _pending_reviews = _load_reviews()
    sess = _pending_reviews.get(st.session_state.sid)
    
    # Auto-refresh if status is "completed" to ensure latest data
    if sess and sess.get("status") == "completed":
        st.success("✅ Batch processing completed! Showing results...")
        time.sleep(0.5)  # Brief delay to ensure UI renders
    
    if sess:
        st.header(f"📋 Reviewing: {sess['table']}")
        
        # Create tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📊 Overview",
            "📈 Columns",
            "🔄 Preview",
            "✏️ Rules",
            "✅ Results",
            "📋 Raw"
        ])
        
        # ========== TAB 1: OVERVIEW ==========
        with tab1:
            st.subheader("Data Statistics")
            
            profile = sess["profile"]
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📊 Total Rows", profile.get("total_rows", 0))
            with col2:
                st.metric("📍 Columns", profile.get("total_columns", 0))
            with col3:
                st.metric("🔐 PII Fields", len(profile.get("pii_fields", [])))
            with col4:
                st.metric("✔️ Rules", len(sess.get("rules", [])))
            
            st.markdown("---")
            st.subheader("Sample Data")
            
            sample = sess.get("sample", [])
            if sample:
                st.dataframe(pd.DataFrame(sample[:5]), use_container_width=True)
            else:
                st.info("No sample data")
            
            if profile.get("pii_fields"):
                st.warning(f"⚠️ **PII Detected:** {', '.join(profile['pii_fields'])}")
        
        # ========== TAB 2: COLUMN ANALYSIS ==========
        with tab2:
            st.subheader("Column-by-Column Analysis")
            
            if "column_stats" in profile and profile["column_stats"]:
                for col_name, col_info in profile["column_stats"].items():
                    with st.expander(f"📌 **{col_name}**", expanded=False):
                        c1, c2, c3 = st.columns(3)
                        
                        with c1:
                            st.metric("Type", col_info.get("dtype", "?"))
                            st.metric("Non-Null", col_info.get("non_null", 0))
                        with c2:
                            st.metric("Missing", col_info.get("missing", 0))
                            st.metric("Missing %", f"{col_info.get('missing_pct', 0):.1f}%")
                        with c3:
                            st.metric("Unique", col_info.get("unique", 0))
                            st.metric("Dupes", col_info.get("duplicate_count", 0))
                        
                        # Numeric stats
                        if col_info.get("mean") is not None or (col_info.get("min") is not None and col_info.get("max") is not None):
                            st.write("**Numeric Stats:**")
                            nc1, nc2, nc3, nc4 = st.columns(4)
                            with nc1:
                                val = col_info.get('mean', 0)
                                st.metric("Mean", f"{val:.2f}" if val is not None else "N/A")
                            with nc2:
                                val = col_info.get('median', 0)
                                st.metric("Median", f"{val:.2f}" if val is not None else "N/A")
                            with nc3:
                                val = col_info.get('min', 0)
                                st.metric("Min", f"{val:.2f}" if val is not None else "N/A")
                            with nc4:
                                val = col_info.get('max', 0)
                                st.metric("Max", f"{val:.2f}" if val is not None else "N/A")
                        
                        samples = col_info.get("sample_values", [])
                        if samples:
                            st.write("**Samples:**")
                            st.code(str(samples[:5]))
            else:
                st.info("No column statistics available")
        
        # ========== TAB 3: TRANSFORMATION PREVIEW ==========
        with tab3:
            st.subheader("🔄 Data Transformation Preview")
            
            # Before/After PII transformation
            if sess.get("preview_before") and sess.get("preview_after"):
                col_before, col_after = st.columns(2)
                
                with col_before:
                    st.write("**BEFORE (Raw):**")
                    st.dataframe(pd.DataFrame(sess["preview_before"][:3]), use_container_width=True)
                
                with col_after:
                    st.write("**AFTER (PII Masked):**")
                    st.dataframe(pd.DataFrame(sess["preview_after"][:3]), use_container_width=True)
                
                pii_cols = profile.get("pii_fields", [])
                if pii_cols:
                    st.success(f"✅ **PII Protection Applied To:** {', '.join(pii_cols)}")
            else:
                st.info("Preview data not available")
            
            st.markdown("---")
            st.subheader("Quality Rules Impact")
            
            if sess.get("preview_failed_rules"):
                for rule_name, info in sess["preview_failed_rules"].items():
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.metric(f"{rule_name} Passed", info.get("passed", 0))
                    with c2:
                        st.metric(f"{rule_name} Failed", info.get("failed", 0))
                    with c3:
                        total = info.get("passed", 0) + info.get("failed", 1)
                        pct = info.get("passed", 0) / total * 100
                        st.metric(f"{rule_name} Pass %", f"{pct:.1f}%")
            else:
                st.success("✅ Preview records pass all rules")
        
        # ========== TAB 4: RULES & APPROVAL ==========
        with tab4:
            st.subheader("📝 Rule Approval")
            
            all_rules = [r.strip() for r in sess.get("rules", []) if r.strip()]
            pii_rules = [r for r in all_rules if ".apply(" in r or "lambda" in r]
            qual_rules = [r for r in all_rules if r not in pii_rules]
            
            c1, c2 = st.columns(2)
            with c1:
                st.info(f"🔄 PII Transformation: {len(pii_rules)} rules")
            with c2:
                st.info(f"✔️ Quality Validation: {len(qual_rules)} rules")
            
            st.markdown("---")
            
            # PII Rules (read-only)
            if pii_rules:
                with st.expander("🔐 PII Transformation Rules (Auto-Generated)", expanded=True):
                    for i, rule in enumerate(pii_rules, 1):
                        st.code(f"# Rule {i}\n{rule[:100]}...", language="python")
                        st.caption(f"{i}/{len(pii_rules)}")
            
            # Quality Rules (read-only)
            if qual_rules:
                with st.expander("✔️ Quality Validation Rules (Auto-Generated)", expanded=True):
                    for i, rule in enumerate(qual_rules, 1):
                        st.code(f"# Rule {i}\n{rule}", language="python")
            
            st.markdown("---")
            st.subheader("Your Decision")
            
            feedback = st.text_area(
                "Feedback (required if rejecting):",
                height=80,
                placeholder="E.g., 'Too many records failing rule #3. Please regenerate with lower sensitivity.'"
            )
            
            col_app, col_rej, col_back = st.columns(3)
            
            with col_app:
                if st.button("✅ APPROVE", key="approve", use_container_width=True):
                    result = submit_review(
                        st.session_state.sid,
                        approved=True,
                        edited_rules=all_rules,
                        feedback="Approved by user"
                    )
                    if result:
                        st.success("✅ Rules approved! Continuing pipeline...")
                        st.session_state.sid = None
                        import time
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Failed to submit approval")
            
            with col_rej:
                if st.button("❌ REJECT", key="reject", use_container_width=True):
                    if not feedback.strip():
                        st.error("Please provide feedback for rejection")
                    else:
                        result = submit_review(
                            st.session_state.sid,
                            approved=False,
                            edited_rules=None,
                            feedback=feedback
                        )
                        if result:
                            st.warning("❌ Rules rejected. Regenerating with your feedback...")
                            st.session_state.sid = None
                            import time
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Failed to submit rejection")
            
            with col_back:
                if st.button("← Go Back", key="back", use_container_width=True):
                    st.session_state.sid = None
                    st.rerun()
        
        # ========== TAB 5: RESULTS & DATA CATALOG ==========
        with tab5:
            st.subheader("📊 Bronze → Silver/Quarantine Data Comparison")
            
            table_name = sess['table']
            
            # Refresh button
            col_refresh_left, col_refresh_right = st.columns([1, 9])
            with col_refresh_left:
                if st.button("🔄 Refresh", key="results_refresh"):
                    st.cache_data.clear()
                    st.rerun()
            with col_refresh_right:
                st.caption("Click to reload CSV files from disk")
            
            st.markdown("---")
            
            # Load actual CSV files
            bronze_path = f"data/bronze/{table_name}.csv"
            silver_path = f"data/silver/{table_name}.csv"
            quarantine_path = f"data/quarantine/{table_name}_quarantine.csv"
            
            df_bronze = load_csv_file(bronze_path)
            df_silver = load_csv_file(silver_path)
            df_quarantine = load_csv_file(quarantine_path)
            
            # Show file statistics
            st.subheader("📈 Data Pipeline Summary")
            
            bronze_count = len(df_bronze) if df_bronze is not None else 0
            silver_count = len(df_silver) if df_silver is not None else 0
            quarantine_count = len(df_quarantine) if df_quarantine is not None else 0
            pass_rate_pct = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🔶 Bronze (Raw)", f"{bronze_count:,}", help="Original data from source")
            with col2:
                st.metric("✅ Silver (Valid)", f"{silver_count:,}", help="Data passing all rules")
            with col3:
                st.metric("⚠️ Quarantine", f"{quarantine_count:,}", help="Data failing validation")
            with col4:
                st.metric("✨ Pass Rate", f"{pass_rate_pct:.1f}%", help="Percentage of valid records")
            
            st.markdown("---")
            st.subheader("❌ Rule Failure Analysis")
            
            # Analyze which rules caused the most failures
            if df_quarantine is not None and 'Failed_Rules' in df_quarantine.columns:
                rule_failure_counts = {}
                for failed_rules_str in df_quarantine['Failed_Rules']:
                    if pd.notna(failed_rules_str):
                        for rule_name in str(failed_rules_str).split('; '):
                            rule_name = rule_name.strip()
                            if rule_name:
                                rule_failure_counts[rule_name] = rule_failure_counts.get(rule_name, 0) + 1
                
                if rule_failure_counts:
                    # Sort by count descending
                    sorted_failures = sorted(rule_failure_counts.items(), key=lambda x: x[1], reverse=True)
                    
                    # Display top failing rules
                    st.write("**Top Rules Causing Failures:**")
                    
                    # Create a bar chart
                    failure_df = pd.DataFrame(sorted_failures, columns=['Rule', 'Failed_Records'])
                    
                    col_chart, col_table = st.columns([2, 1])
                    
                    with col_chart:
                        st.bar_chart(
                            data=failure_df.set_index('Rule'),
                            use_container_width=True,
                            height=300
                        )
                    
                    with col_table:
                        st.write("**Failure Count:**")
                        for rule, count in sorted_failures[:5]:
                            pct = (count / quarantine_count * 100) if quarantine_count > 0 else 0
                            st.metric(rule, f"{count:,}", f"{pct:.1f}% of failed")
                else:
                    st.info("No rule failure details available in quarantine data")
            else:
                st.info("No quarantine records - no failures to analyze")
            
            st.markdown("---")
            st.subheader("📋 Data Transformation Pipeline")
            
            # Three-column layout: Bronze | Silver | Quarantine
            st.write("**Detailed Data View (First 10 rows from each):**")
            
            col_bronze, col_silver, col_quarantine = st.columns(3)
            
            # BRONZE (Original Data)
            with col_bronze:
                st.write("### 🔶 BRONZE (Raw)")
                st.caption(f"{bronze_count:,} total records")
                
                if df_bronze is not None:
                    st.info(f"✅ File exists: `{bronze_path}`")
                    
                    # Show first 10 rows
                    st.write("**First 10 rows:**")
                    st.dataframe(
                        df_bronze.head(10),
                        use_container_width=True,
                        height=400
                    )
                    
                    # Show columns
                    with st.expander("📊 Column Info"):
                        st.write(df_bronze.dtypes)
                    
                    # Download button
                    with open(bronze_path, 'rb') as f:
                        st.download_button(
                            label="⬇️ Download Bronze (Raw)",
                            data=f,
                            file_name=f"{table_name}_bronze.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                else:
                    st.warning(f"❌ Bronze file not found: `{bronze_path}`")
            
            # SILVER (Validated Data)
            with col_silver:
                st.write("### ✅ SILVER (Valid)")
                st.caption(f"{silver_count:,} total records (passed)")
                
                if df_silver is not None:
                    st.success(f"✅ File exists: `{silver_path}`")
                    
                    # Show first 10 rows
                    st.write("**First 10 rows:**")
                    st.dataframe(
                        df_silver.head(10),
                        use_container_width=True,
                        height=400
                    )
                    
                    # Show columns
                    with st.expander("📊 Column Info"):
                        st.write(df_silver.dtypes)
                    
                    # Download button
                    with open(silver_path, 'rb') as f:
                        st.download_button(
                            label="⬇️ Download Silver (Valid)",
                            data=f,
                            file_name=f"{table_name}_silver.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                else:
                    st.warning(f"❌ Silver file not found: `{silver_path}`")
            
            # QUARANTINE (Failed Data)
            with col_quarantine:
                st.write("### ⚠️ QUARANTINE (Failed)")
                st.caption(f"{quarantine_count:,} total records (failed)")
                
                if df_quarantine is not None:
                    st.warning(f"⚠️ File exists: `{quarantine_path}`")
                    
                    # Show first 10 rows
                    st.write("**First 10 rows:**")
                    
                    # Highlight the Failed_Rules column
                    df_display = df_quarantine.head(10).copy()
                    if 'Failed_Rules' in df_display.columns:
                        st.info("📌 **Failed_Rules column** shows which rules each row violated")
                    
                    st.dataframe(
                        df_display,
                        use_container_width=True,
                        height=400
                    )
                    
                    # Show columns
                    with st.expander("📊 Column Info"):
                        st.write(df_quarantine.dtypes)
                    
                    # Download button
                    with open(quarantine_path, 'rb') as f:
                        st.download_button(
                            label="⬇️ Download Quarantine (Failed)",
                            data=f,
                            file_name=f"{table_name}_quarantine.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                else:
                    st.info(f"ℹ️ No quarantine file (all records passed!)")
            
            st.markdown("---")
            st.subheader("🎯 Transformation Summary")
            
            summary_col1, summary_col2, summary_col3 = st.columns(3)
            
            with summary_col1:
                st.write(f"**🔶 Bronze Records:** {bronze_count:,}")
                st.write("*Original raw data from source*")
            
            with summary_col2:
                st.write(f"**✅ Silver Records:** {silver_count:,}")
                st.write("*Data passing all validation rules*")
            
            with summary_col3:
                st.write(f"**⚠️ Quarantine Records:** {quarantine_count:,}")
                st.write("*Data failing validation rules*")
            
            st.success(f"""
            ### ✅ Transformation Complete
            - **Bronze (Raw):** {bronze_count:,} records
            - **Silver (Valid):** {silver_count:,} records ({pass_rate_pct:.1f}% pass rate)
            - **Quarantine (Failed):** {quarantine_count:,} records ({100-pass_rate_pct:.1f}% fail rate)
            - **Quality Score:** {pass_rate_pct:.1f}/100
            """)
        
        # ========== TAB 6: RAW DATA ==========
        with tab6:
            st.subheader("Raw Data (Debug)")
            
            with st.expander("Profile Data", expanded=False):
                st.json(sess["profile"])
            
            with st.expander("First 5 Sample Rows", expanded=False):
                if sess.get("sample"):
                    st.json(sess["sample"][:5])
    
    else:
        st.error(f"Session not found: {st.session_state.sid}")
        if st.button("Go Back"):
            st.session_state.sid = None
            st.rerun()
