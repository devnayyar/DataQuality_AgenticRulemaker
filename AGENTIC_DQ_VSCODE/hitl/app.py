# hitl/app.py - DQ Rule Approval UI
import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import json
from typing import List, Dict, Any

# --- Page Configuration ---
st.set_page_config(
    page_title="DQ Rule Approval",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Path Setup ---
# Set the root directory and add it to the Python path for correct module imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

# --- Imports ---
from hitl.controller import (create_review, submit_review, _load_reviews)
from execution.rule_enforcer import apply_rules
from llm.rule_validator import validate_rules
from profiling.pii_transformer import apply_pii_transformations

# --- Custom Styling ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .success-text { color: #09ab3b; font-weight: bold; }
    .warning-text { color: #ff2b2b; font-weight: bold; }
    .info-text { color: #0668bc; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

st.title("📊 Data Quality Rule Approval")
st.markdown("---")

# Initialize session ID state
if "sid" not in st.session_state:
    st.session_state.sid = None

# --- SECTION 1: Select a Pending Review Session (if no session is active) ---
if st.session_state.sid is None:
    
    # Load reviews fresh each time (important for multi-user scenarios)
    _pending_reviews = _load_reviews()
    
    # Get all review SIDs that are currently pending
    # This checks the global _pending_reviews dictionary populated by batch_runner.py
    pending_sids = [sid for sid, sess in _pending_reviews.items() if sess["status"] == "pending"]
    
    if not pending_sids:
        st.info("No pending review sessions found. Please run 'jobs/batch_runner.py' first.")
        # Optional: You could re-add the old demo-creation logic here if needed.
        
    else:
        # Create a dictionary mapping Table Name to Session ID for the dropdown display
        session_options = {
            _pending_reviews[sid]['table']: sid
            for sid in pending_sids
        }
        
        # Display the dropdown of tables with pending rules
        selected_table = st.selectbox(
            "Select Table with Pending Rules to Review", 
            list(session_options.keys())
        )
        
        if st.button(f"Load Rules for {selected_table}"):
            # Set the SID based on the selected table name
            st.session_state.sid = session_options[selected_table]
            st.rerun()

# --- SECTION 2: Display and Process the Active Review Session ---
if st.session_state.sid:
    # Reload reviews fresh to ensure we have the latest state
    _pending_reviews = _load_reviews()
    sess = _pending_reviews.get(st.session_state.sid)
    
    # Check if the session still exists (e.g., in case of a crash/cleanup)
    if sess:
        st.header(f"📋 Table: {sess['table']}")
        
        # --- TAB 1: DATA OVERVIEW ---
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Data Overview",
            "🔍 Rule Preview",
            "✏️ Edit Rules",
            "⚙️ Raw Data"
        ])
        
        with tab1:
            st.subheader("Profiling Statistics")
            
            profile = sess["profile"]
            sample = sess["sample"]
            
            # Create columns for key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_rows = profile.get("row_count", 0)
                st.metric("📈 Total Rows", f"{total_rows:,}")
            
            with col2:
                num_columns = len(sample[0]) if sample else 0
                st.metric("📋 Columns", num_columns)
            
            with col3:
                missing_count = profile.get("missing_count", 0)
                missing_pct = (missing_count / (total_rows * num_columns) * 100) if (total_rows * num_columns) > 0 else 0
                st.metric("❌ Missing Values", f"{missing_count} ({missing_pct:.1f}%)")
            
            with col4:
                duplicates = profile.get("duplicate_count", 0)
                st.metric("🔄 Duplicates", duplicates)
            
            st.markdown("---")
            
            # Column-wise statistics
            st.subheader("Column Analysis")
            
            if "column_stats" in profile:
                col_stats = profile["column_stats"]
                stats_data = []
                
                for col_name, col_info in col_stats.items():
                    stats_data.append({
                        "Column": col_name,
                        "Data Type": col_info.get("dtype", "unknown"),
                        "Non-Null": col_info.get("non_null", 0),
                        "Missing": col_info.get("missing", 0),
                        "Unique": col_info.get("unique", 0),
                        "Examples": str(col_info.get("sample_values", [])[:2])[:50]
                    })
                
                stats_df = pd.DataFrame(stats_data)
                st.dataframe(stats_df, width='stretch')
            
            st.markdown("---")
            st.subheader("Data Sample (First 5 Rows)")
            
            # Display sample as dataframe for better readability
            if sample:
                sample_df = pd.DataFrame(sample[:5])
                st.dataframe(sample_df, width='stretch')
            else:
                st.info("No sample data available")
            
            # PII Information
            if "pii_fields" in profile and profile["pii_fields"]:
                st.warning(f"⚠️ PII Detected in columns: {', '.join(profile['pii_fields'])}")
        
        # --- TAB 2: RULE PREVIEW & IMPACT ---
        with tab2:
            st.subheader("Rule Impact Analysis")
            
            # Get the current rules and separate them
            rules_text = "\n".join(sess["rules"])
            all_rules = [r.strip() for r in rules_text.split("\n") if r.strip()]
            
            # Separate transformation rules from validation rules
            transformation_rules = [r for r in all_rules if ".apply(" in r or "=" in r.split("df")[0] if "df" in r]
            validation_rules = [r for r in all_rules if r not in transformation_rules]
            
            # Show rule types
            col_trans, col_valid = st.columns(2)
            with col_trans:
                st.info(f"🔄 Transformation Rules: {len(transformation_rules)}")
            with col_valid:
                st.info(f"✔️ Validation Rules: {len(validation_rules)}")
            
            st.markdown("---")
            
            if not validation_rules:
                st.warning("⚠️ No validation rules to preview (only PII transformations)")
                if transformation_rules:
                    st.info("**Transformation Rules (will be applied to mask PII):**")
                    for rule in transformation_rules[:5]:
                        st.code(rule, language="python")
                    if len(transformation_rules) > 5:
                        st.caption(f"... and {len(transformation_rules)-5} more transformation rule(s)")
            else:
                st.info(f"📌 Analyzing {len(validation_rules)} validation rule(s) on sample data...")
                
                try:
                    # Create a dataframe from sample for analysis
                    sample_df = pd.DataFrame(sess["sample"])
                    pii_fields = sess["profile"].get("pii_fields", [])
                    
                    # STEP 1: Apply transformations first
                    if transformation_rules and pii_fields:
                        st.info("📝 Applying PII transformations to sample...")
                        sample_df = apply_pii_transformations(sample_df, pii_fields)
                        st.success("✅ PII transformations applied")
                        st.markdown("---")
                    
                    # STEP 2: Preview validation rules on transformed data
                    initial_count = len(sample_df)
                    
                    try:
                        # Evaluate validation rules as boolean expressions
                        mask = pd.Series([True] * len(sample_df), index=sample_df.index)
                        failed_rules_preview = []
                        
                        for idx, rule in enumerate(validation_rules):
                            try:
                                rule_mask = eval(rule, {"df": sample_df, "pd": pd})
                                if isinstance(rule_mask, (pd.Series, bool)):
                                    mask = mask & rule_mask
                                else:
                                    failed_rules_preview.append({"index": idx, "rule": rule, "error": "Did not return boolean"})
                            except Exception as e:
                                failed_rules_preview.append({"index": idx, "rule": rule, "error": str(e)})
                        
                        passed_count = mask.sum()
                        failed_count = (~mask).sum()
                        pass_rate = (passed_count / initial_count * 100) if initial_count > 0 else 0
                        
                        # Show impact metrics
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("✅ Clean Records", int(passed_count))
                        with col2:
                            st.metric("🚫 Quarantined", int(failed_count))
                        with col3:
                            st.metric("📊 Pass Rate", f"{pass_rate:.1f}%")
                        with col4:
                            st.metric("⚠️ Failed Rules", len(failed_rules_preview))
                        
                        st.markdown("---")
                        
                        # Show before/after comparison
                        col_before, col_after = st.columns(2)
                        
                        with col_before:
                            st.subheader("❌ Records to Quarantine")
                            bad_df = sample_df[~mask]
                            
                            if not bad_df.empty:
                                st.dataframe(
                                    bad_df.head(10),
                                    width='stretch',
                                    height=400
                                )
                                if len(bad_df) > 10:
                                    st.caption(f"Showing 10 of {len(bad_df)} quarantined records")
                            else:
                                st.success("✅ All records pass validation!")
                        
                        with col_after:
                            st.subheader("✅ Clean Data")
                            good_df = sample_df[mask]
                            
                            if not good_df.empty:
                                st.dataframe(
                                    good_df.head(10),
                                    width='stretch',
                                    height=400
                                )
                                if len(good_df) > 10:
                                    st.caption(f"Showing 10 of {len(good_df)} clean records")
                            else:
                                st.warning("⚠️ No records pass validation")
                        
                        # Show failed rules
                        if failed_rules_preview:
                            st.markdown("---")
                            st.subheader("⚠️ Rules with Errors")
                            for rule_info in failed_rules_preview:
                                rule = rule_info.get("rule", "Unknown")
                                error = rule_info.get("error", "Unknown error")
                                with st.expander(f"Rule {rule_info.get('index', '?')}: {rule[:60]}..."):
                                    st.error(f"Error: {error}")
                        
                        st.success("✅ Rule preview complete!")
                        
                    except Exception as e:
                        st.error(f"❌ Error evaluating validation rules: {str(e)}")
                        
                except Exception as e:
                    st.error(f"❌ Error during preview: {str(e)}")
        
        # --- TAB 3: EDIT RULES ---
        with tab3:
            st.subheader("Generated Rules")
            
            # Validate rules
            rules_text = "\n".join(sess["rules"])
            all_rules = [r.strip() for r in rules_text.split("\n") if r.strip()]
            
            # Separate transformation and validation rules
            transformation_rules = [r for r in all_rules if ".apply(" in r or ("=" in r.split("df")[0] if "df" in r else False)]
            validation_rules = [r for r in all_rules if r not in transformation_rules]
            
            st.write("**📋 Rule Summary:**")
            col_t, col_v = st.columns(2)
            with col_t:
                st.info(f"🔄 Transformation Rules: {len(transformation_rules)}")
            with col_v:
                st.info(f"✔️ Validation Rules: {len(validation_rules)}")
            
            st.markdown("---")
            
            # Show rules by type
            if transformation_rules:
                st.write("**🔄 PII Transformation Rules** (Applied first to mask/hash sensitive data)")
                for i, rule in enumerate(transformation_rules, 1):
                    st.code(rule, language="python")
            
            if validation_rules:
                st.write("**✔️ Data Validation Rules** (Applied after transformations)")
                for i, rule in enumerate(validation_rules, 1):
                    st.code(rule, language="python")
            
            st.markdown("---")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write("**📝 Review and edit rules below (one per line):**")
            with col2:
                validate_btn = st.button("🔍 Validate Syntax")
            
            edited_rules = st.text_area(
                "Rules (One Per Line)",
                rules_text,
                height=350,
                key="rules"
            )
            
            # Syntax validation
            if validate_btn:
                clean_rules = [r.strip() for r in edited_rules.split("\n") if r.strip()]
                if clean_rules:
                    try:
                        # Only validate non-transformation rules
                        rules_to_validate = [r for r in clean_rules if ".apply(" not in r]
                        if rules_to_validate:
                            validate_rules(rules_to_validate)
                            st.success("✅ All validation rules are syntactically correct!")
                        else:
                            st.success("✅ Only transformation rules (syntax auto-checked)")
                    except Exception as e:
                        st.error(f"❌ Validation Error: {str(e)}")
            
            st.markdown("---")
            st.subheader("Feedback")
            feedback = st.text_area(
                "Additional Feedback (Required if Rejecting)",
                height=100,
                key="feedback"
            )
            
            st.markdown("---")
            st.subheader("Decision")
            
            col1, col2 = st.columns(2)
            
            # Approval Logic
            with col1:
                if st.button("✅ Approve Rules", type="primary", width='stretch'):
                    # Clean empty lines from edited rules
                    clean_rules = [r.strip() for r in edited_rules.split("\n") if r.strip()]
                    submit_review(st.session_state.sid, True, clean_rules, feedback)
                    st.success("✅ Rules Approved! Workflow continuing...")
                    st.session_state.sid = None
                    st.balloons()
                    st.rerun()
                    
            # Rejection Logic
            with col2:
                if st.button("❌ Reject Rules", type="secondary", width='stretch'):
                    if not feedback:
                        st.error("⚠️ Feedback is required to reject rules.")
                    else:
                        # Note: When rejecting, final_rules is None, prompting regeneration
                        submit_review(st.session_state.sid, False, None, feedback) 
                        st.warning("❌ Rules Rejected. Feedback sent for regeneration...")
                        st.session_state.sid = None
                        st.rerun()
        
        # --- TAB 4: RAW DATA (JSON) ---
        with tab4:
            st.subheader("Raw Profile Data")
            st.json(sess["profile"])
            
            st.subheader("Raw Sample Data")
            st.json(sess["sample"])