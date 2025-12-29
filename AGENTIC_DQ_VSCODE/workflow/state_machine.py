# workflow/state_machine.py

import logging
import time
import hashlib
from typing import Literal, Optional, Dict, Any, List

import pandas as pd
import numpy as np

from langgraph.graph import StateGraph, START, END

from profiling.statistical_profiler import generate_profile
from profiling.pii_detector import detect_pii, detect_pii_with_types
from llm.rule_generator import generate_pii_rules, generate_general_rules
from llm.rule_validator import validate_rules
from llm.feedback_loop import incorporate_feedback
from execution.rule_enforcer import apply_rules, apply_rules_with_pii_transformation
from evaluation.scorer import score_rules, send_email_alert
from hitl.controller import create_review, _load_reviews


# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------------------------------------
# State Definition
# -------------------------------------------------
class DQState(dict):
    df: pd.DataFrame
    table_name: str

    profile: Dict[str, Any]
    sample: List[Dict[str, Any]]
    pii: List[str]
    pii_types: Dict[str, str]  # Maps PII field to entity type (e.g., 'email': 'EMAIL_ADDRESS')
    schema: str

    pii_rules: List[str]          # PII transformation rules (exec-based, dynamically generated)
    general_rules: List[str]      # General validation rules (boolean expressions, comprehensive)
    rules: List[str]              # Combined rules for HITL review
    feedback: Optional[str]

    hitl_status: Optional[str]       # pending | approved | rejected
    hitl_session_id: Optional[str]

    metrics: Optional[Dict[str, Any]]
    score: Optional[str]  # Score is a string (LLM generated text)


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def clean_for_json(obj):
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    if isinstance(obj, (np.ndarray, pd.Series)):
        return obj.tolist()
    return obj


# -------------------------------------------------
# Nodes
# -------------------------------------------------
def profile_node(state: DQState):
    logger.info("=" * 60)
    logger.info("STEP 1: Profiling data and detecting PII")
    logger.info("=" * 60)

    df = state["df"]
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Total columns: {len(df.columns)}")

    profile = clean_for_json(generate_profile(df))
    sample = df.head(10).to_dict("records")
    pii, pii_types = detect_pii_with_types(sample)  # Get both field names and types

    logger.info(f"✅ Data profiled: {profile.get('total_rows')} rows, {profile.get('total_columns')} columns")
    logger.info(f"✅ PII columns detected: {pii}")
    logger.info(f"✅ PII types: {pii_types}")

    return {
        **state,
        "profile": profile,
        "sample": sample,
        "pii": pii,
        "pii_types": pii_types,  # Pass PII types for dynamic rule generation
        "schema": str(df.dtypes),
        "df_original": df.copy()  # Store original for comparison
    }


def generate_node(state: DQState):
    logger.info("=" * 60)
    logger.info("STEP 2: Generating dynamic DQ rules")
    logger.info("=" * 60)

    # Generate DYNAMIC PII transformation rules using LLM (based on detected PII types)
    logger.info(f"Generating PII transformation rules for {len(state['pii'])} field(s)...")
    pii_rules = generate_pii_rules(state["pii"], state.get("pii_types", {}))
    logger.info(f"✅ Generated {len(pii_rules)} PII transformation rule(s)")
    
    # Generate COMPREHENSIVE general validation rules across all non-PII columns
    logger.info(f"Generating quality validation rules for {len(state['schema'].split())} column(s)...")
    general_rules = generate_general_rules(
        state["schema"],
        state["profile"],
        state["pii"]  # Pass PII fields to exclude from validation rules
    )
    logger.info(f"✅ Generated {len(general_rules)} quality validation rule(s)")

    # Separate PII and general rules for processing
    rules = pii_rules + general_rules
    ok, err = validate_rules(general_rules)  # Only validate general rules (PII rules are exec-based)

    if not ok:
        logger.error(f"Rule validation failed: {err}")
        general_rules = []

    logger.info(f"Total rules generated: {len(pii_rules)} (PII) + {len(general_rules)} (Quality) = {len(rules)}")

    return {
        **state,
        "pii_rules": pii_rules,
        "general_rules": general_rules,
        "rules": rules,  # Combined for HITL display
        "hitl_status": "pending",
        "hitl_session_id": None,
        "feedback": None
    }


def preview_transformations_node(state: DQState):
    """
    PREVIEW STEP: Apply transformations to sample data FIRST to show user impact
    """
    logger.info("=" * 60)
    logger.info("STEP 3: Previewing PII transformations on sample")
    logger.info("=" * 60)
    
    # Create a sample dataframe to preview
    sample_df = pd.DataFrame(state["sample"])
    logger.info(f"Preview on {len(sample_df)} sample rows")
    
    # Store original sample for comparison
    preview_before = sample_df.copy()
    preview_after = sample_df.copy()
    
    # Apply PII transformations to preview
    if state["pii_rules"] and len(state["pii"]) > 0:
        logger.info(f"Applying {len(state['pii_rules'])} PII transformation rules to preview...")
        
        pii_transform_count = 0
        for idx, rule in enumerate(state["pii_rules"], 1):
            try:
                exec(rule, {"df": preview_after, "pd": pd, "hashlib": __import__("hashlib")})
                pii_transform_count += 1
                logger.info(f"  ✓ PII rule {idx}/{len(state['pii_rules'])}: {rule[:60]}...")
            except Exception as e:
                logger.warning(f"  ✗ PII rule {idx} failed (will retry on full data): {str(e)[:50]}")
                pass
        
        logger.info(f"✅ Applied {pii_transform_count}/{len(state['pii_rules'])} PII rules to preview")
    
    # Quality rules preview (show which records would fail)
    if state["general_rules"]:
        logger.info(f"Previewing {len(state['general_rules'])} quality validation rules on transformed data...")
        
        failed_rules = {}
        for idx, rule in enumerate(state["general_rules"], 1):
            try:
                result = eval(rule, {"df": preview_after, "pd": pd, "np": __import__("numpy")})
                passed = result.sum() if hasattr(result, 'sum') else (result.sum() if isinstance(result, list) else int(result))
                failed = len(preview_after) - passed
                
                if failed > 0:
                    failed_rules[f"Rule {idx}"] = {"failed": failed, "passed": passed, "rule": rule[:80]}
                    logger.info(f"  Rule {idx}: ✓ {passed} passed, ✗ {failed} failed - {rule[:60]}...")
                else:
                    logger.info(f"  Rule {idx}: ✓ All {passed} records passed - {rule[:60]}...")
            except Exception as e:
                logger.warning(f"  Rule {idx} preview error: {str(e)[:50]}")
                pass
    
    logger.info("✅ Preview complete - showing results to user for approval")
    
    return {
        **state,
        "preview_before": preview_before.to_dict("records"),
        "preview_after": preview_after.to_dict("records"),
        "preview_failed_rules": failed_rules if state["general_rules"] else {}
    }


def hitl_node(state: DQState):
    logger.info("=" * 60)
    logger.info("STEP 4: Sending to HITL for approval")
    logger.info("=" * 60)

    # Create review session ONLY ONCE
    if not state.get("hitl_session_id"):
        sid = create_review(
            table_name=state["table_name"],
            rules=state["rules"],
            profile=state["profile"],
            sample=state.get("preview_before", state["sample"]),
            preview_after=state.get("preview_after"),
            preview_failed_rules=state.get("preview_failed_rules")
        )
        logger.warning(f"HITL review created (SID={sid}). Waiting for user approval...")

        return {
            **state,
            "hitl_session_id": sid,
            "hitl_status": "pending"
        }

    # Poll current status with retries - RELOAD FROM DISK each time
    max_retries = 120  # ~10 minutes with 5-second intervals
    retry_count = 0
    while retry_count < max_retries:
        # CRUCIAL: Reload from disk each iteration to pick up Streamlit changes
        pending_reviews = _load_reviews()
        sess = pending_reviews.get(state["hitl_session_id"])
        
        if sess and sess["status"] != "pending":
            break
        retry_count += 1
        if retry_count < max_retries:
            logger.info(f"Waiting for HITL approval... (attempt {retry_count}/{max_retries})")
            time.sleep(5)  # Wait 5 seconds before retrying
    
    pending_reviews = _load_reviews()
    sess = pending_reviews.get(state["hitl_session_id"])
    if not sess:
        logger.error(f"Review session {state['hitl_session_id']} not found")
        return state

    if sess["status"] == "approved":
        logger.info("✅ HITL APPROVED the rules")
        approved_rules = sess.get("final_rules", state["rules"])
        
        # Split rules back into PII and general based on content
        pii_rules_approved = [r for r in approved_rules if ".apply(" in r or "lambda" in r]
        general_rules_approved = [r for r in approved_rules if r not in pii_rules_approved]
        
        logger.info(f"Using {len(pii_rules_approved)} PII rules and {len(general_rules_approved)} quality rules")
        
        return {
            **state,
            "pii_rules": pii_rules_approved,
            "general_rules": general_rules_approved,
            "hitl_status": "approved"
        }

    if sess["status"] == "rejected":
        logger.warning(f"❌ HITL REJECTED the rules")
        logger.warning(f"Feedback: {sess.get('feedback', 'None')}")
        return {
            **state,
            "feedback": sess.get("feedback"),
            "hitl_status": "rejected",
            "hitl_session_id": None  # Reset for regeneration
        }

    return state


def regenerate_node(state: DQState):
    logger.info("=" * 60)
    logger.info("Regenerating rules using HITL feedback")
    logger.info("=" * 60)

    new_rules = incorporate_feedback(
        state["rules"],
        state["feedback"]
    )
    
    # Validate regenerated rules
    ok, err = validate_rules(new_rules)
    if not ok:
        logger.error(f"Regenerated rule validation failed: {err}")
        new_rules = []

    return {
        **state,
        "rules": new_rules,
        "feedback": None,
        "hitl_status": "pending",
        "hitl_session_id": None
    }


def apply_node(state: DQState):
    logger.info("=" * 60)
    logger.info("STEP 5: Applying approved rules to full dataset")
    logger.info("=" * 60)

    df = state["df"].copy()
    logger.info(f"Starting with {len(df)} total records")

    # STEP 1: Apply PII transformations
    if state.get("pii_rules") and len(state["pii"]) > 0:
        logger.info(f"\n--- Step 5.1: Applying {len(state['pii_rules'])} PII transformation rule(s) ---")
        
        pii_success = 0
        for idx, rule in enumerate(state["pii_rules"], 1):
            try:
                exec(rule, {"df": df, "pd": pd, "hashlib": hashlib})
                pii_success += 1
                logger.info(f"  ✓ PII rule {idx}/{len(state['pii_rules'])}: {rule[:70]}...")
            except Exception as e:
                logger.warning(f"  ✗ PII rule {idx} failed: {str(e)[:60]}")
        
        logger.info(f"✅ Applied {pii_success}/{len(state['pii_rules'])} PII transformations")
        logger.info(f"   Records after PII transformation: {len(df)}")
    else:
        logger.info("No PII transformations needed (no PII fields detected)")

    # STEP 2: Apply quality validation rules (with validation to skip bad rules)
    if state.get("general_rules"):
        logger.info(f"\n--- Step 5.2: Evaluating {len(state['general_rules'])} quality validation rule(s) ---")
        
        # Track which records pass all rules
        passing_records = pd.Series([True] * len(df), index=df.index)
        rule_results = {}
        valid_rules_applied = 0
        
        for idx, rule in enumerate(state["general_rules"], 1):
            try:
                result = eval(rule, {"df": df, "pd": pd, "np": np})
                
                # Count pass/fail
                if isinstance(result, pd.Series):
                    passed = result.sum()
                    failed = (~result).sum()
                elif isinstance(result, pd.Series):
                    passed = result.astype(bool).sum()
                    failed = (~result).sum()
                elif isinstance(result, (list, np.ndarray)):
                    passed = sum(result)
                    failed = len(result) - passed
                else:
                    # Single boolean result
                    passed = 1 if result else 0
                    failed = 0 if result else 1
                
                pass_rate = (passed / len(df) * 100) if len(df) > 0 else 0
                
                # VALIDATION: Skip rules that fail all records (likely malformed)
                if pass_rate < 5:  # Less than 5% pass rate = probably bad rule
                    logger.warning(f"  ⊘ Rule {idx} SKIPPED (only {pass_rate:.1f}% pass rate, likely malformed)")
                    rule_results[f"Rule {idx}"] = {"passed": int(passed), "failed": int(failed), "status": "SKIPPED_LOW_PASS"}
                    continue
                
                # VALIDATION: Skip rules with 100% pass rate (likely too lenient or constant)
                if pass_rate > 99:
                    logger.warning(f"  ⊘ Rule {idx} SKIPPED ({pass_rate:.1f}% pass rate, too lenient)")
                    rule_results[f"Rule {idx}"] = {"passed": int(passed), "failed": int(failed), "status": "SKIPPED_TOO_LENIENT"}
                    continue
                
                # Track which records fail this rule
                if isinstance(result, pd.Series):
                    passing_records = passing_records & result
                    valid_rules_applied += 1
                
                rule_results[f"Rule {idx}"] = {"passed": int(passed), "failed": int(failed), "pass_rate": pass_rate, "status": "APPLIED"}
                logger.info(f"  Rule {idx}: ✓ {int(passed)} passed, ✗ {int(failed)} failed ({pass_rate:.1f}%) - {rule[:60]}...")
                
            except Exception as e:
                logger.warning(f"  ✗ Rule {idx} evaluation failed: {str(e)[:80]}")
                rule_results[f"Rule {idx}"] = {"passed": 0, "failed": len(df), "error": str(e)[:60], "status": "ERROR"}

        logger.info(f"   Applied {valid_rules_applied}/{len(state['general_rules'])} quality rules (skipped {len(state['general_rules']) - valid_rules_applied} malformed/lenient rules)")

        # Partition data
        silver_df = df[passing_records]
        quarantine_df = df[~passing_records]
        
        logger.info(f"\n--- Step 5.3: Partitioning data ---")
        logger.info(f"  ✅ SILVER (all rules passed):     {len(silver_df):,} records ({100*len(silver_df)/len(df):.2f}%)")
        logger.info(f"  ⚠️  QUARANTINE (rule failures):   {len(quarantine_df):,} records ({100*len(quarantine_df)/len(df):.2f}%)")
    else:
        logger.info("No quality validation rules - all records go to Silver")
        silver_df = df
        quarantine_df = pd.DataFrame()
        rule_results = {}

    # Save results
    logger.info(f"\n--- Step 5.4: Saving results ---")
    silver_path = f"data/silver/{state['table_name']}.csv"
    quarantine_path = f"data/quarantine/{state['table_name']}_quarantine.csv"
    
    silver_df.to_csv(silver_path, index=False)
    logger.info(f"  ✓ Saved Silver to: {silver_path}")
    
    if len(quarantine_df) > 0:
        quarantine_df.to_csv(quarantine_path, index=False)
        logger.info(f"  ✓ Saved Quarantine to: {quarantine_path}")
    else:
        logger.info(f"  ✓ No quarantined records")

    # Metrics
    metrics = {
        "total": len(df),
        "passed": len(silver_df),
        "failed": len(quarantine_df),
        "pass_rate": len(silver_df) / len(df) if len(df) > 0 else 0,
        "rule_details": rule_results
    }

    logger.info(f"\n✅ STEP 5 COMPLETE: Rules applied successfully")
    logger.info(f"   Pass Rate: {100*metrics['pass_rate']:.2f}%")

    score = score_rules(
        state.get("general_rules", []),
        state["pii"],
        metrics
    )

    send_email_alert(
        state["table_name"],
        metrics,
        score
    )

    return {
        **state,
        "metrics": metrics,
        "score": score
    }


# -------------------------------------------------
# Conditional Router
# -------------------------------------------------
def approval_router(state: DQState) -> Literal[
    "apply",
    "regenerate",
    "hitl",
    END
]:
    status = state.get("hitl_status")

    if status == "approved":
        return "apply"

    if status == "rejected":
        return "regenerate"

    if status == "pending":
        return "hitl"  # Loop back to keep polling

    return END


# -------------------------------------------------
# Build Graph
# -------------------------------------------------
def build_workflow():
    graph = StateGraph(DQState)

    graph.add_node("profile_data", profile_node)
    graph.add_node("generate", generate_node)
    graph.add_node("preview_transformations", preview_transformations_node)
    graph.add_node("hitl", hitl_node)
    graph.add_node("regenerate", regenerate_node)
    graph.add_node("apply", apply_node)

    graph.add_edge(START, "profile_data")
    graph.add_edge("profile_data", "generate")
    graph.add_edge("generate", "preview_transformations")
    graph.add_edge("preview_transformations", "hitl")

    graph.add_conditional_edges(
        "hitl",
        approval_router,
        ["apply", "regenerate", "hitl", END]
    )

    graph.add_edge("regenerate", "preview_transformations")
    graph.add_edge("apply", END)

    return graph.compile()
