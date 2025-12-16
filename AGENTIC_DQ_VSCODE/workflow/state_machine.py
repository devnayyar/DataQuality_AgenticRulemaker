# workflow/state_machine.py

import logging
import time
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
    logger.info("Profiling data and detecting PII")

    df = state["df"]

    profile = clean_for_json(generate_profile(df))
    sample = df.head(10).to_dict("records")
    pii, pii_types = detect_pii_with_types(sample)  # Get both field names and types

    return {
        **state,
        "profile": profile,
        "sample": sample,
        "pii": pii,
        "pii_types": pii_types,  # Pass PII types for dynamic rule generation
        "schema": str(df.dtypes)
    }


def generate_node(state: DQState):
    logger.info("Generating DQ rules (both PII transformation and general quality)")

    # Generate DYNAMIC PII transformation rules using LLM (based on detected PII types)
    pii_rules = generate_pii_rules(state["pii"], state.get("pii_types", {}))
    
    # Generate COMPREHENSIVE general validation rules across all non-PII columns
    general_rules = generate_general_rules(
        state["schema"],
        state["profile"],
        state["pii"]  # Pass PII fields to exclude from validation rules
    )

    # Separate PII and general rules for processing
    rules = pii_rules + general_rules
    ok, err = validate_rules(general_rules)  # Only validate general rules (PII rules are exec-based)

    if not ok:
        logger.error(f"Rule validation failed: {err}")
        general_rules = []

    return {
        **state,
        "pii_rules": pii_rules,
        "general_rules": general_rules,
        "rules": rules,  # Combined for HITL display
        "hitl_status": "pending",
        "hitl_session_id": None,
        "feedback": None
    }


def hitl_node(state: DQState):
    logger.info("HITL node reached")

    # Create review session ONLY ONCE
    if not state.get("hitl_session_id"):
        sid = create_review(
            table_name=state["table_name"],
            rules=state["rules"],
            profile=state["profile"],
            sample=state["sample"]
        )
        logger.warning(f"HITL review created (SID={sid}). Waiting for approval...")

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
        return state

    if sess["status"] == "approved":
        logger.info("HITL approved rules")
        approved_rules = sess["final_rules"]
        
        # Split rules back into PII and general based on content
        pii_rules_approved = [r for r in approved_rules if "apply(lambda" in r or ".apply(" in r]
        general_rules_approved = [r for r in approved_rules if r not in pii_rules_approved]
        
        return {
            **state,
            "rules": approved_rules,
            "pii_rules": pii_rules_approved,
            "general_rules": general_rules_approved,
            "hitl_status": "approved"
        }

    if sess["status"] == "rejected":
        logger.warning("HITL rejected rules")
        return {
            **state,
            "feedback": sess["feedback"],
            "hitl_status": "rejected"
        }

    return state


def regenerate_node(state: DQState):
    logger.info("Regenerating rules using HITL feedback")

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
    logger.info("Applying rules: PII Transformations → Quality Validation → Silver")

    result = apply_rules_with_pii_transformation(
        df=state["df"],
        table_name=state["table_name"],
        pii_fields=state["pii"],
        pii_rules=state.get("pii_rules", []),
        general_rules=state.get("general_rules", [])
    )

    # Extract metrics from result
    metrics = {
        "total": result["total"],
        "passed": result["passed"],
        "failed": result["failed"],
        "pass_rate": result["pass_rate"]
    }

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
    graph.add_node("hitl", hitl_node)
    graph.add_node("regenerate", regenerate_node)
    graph.add_node("apply", apply_node)

    graph.add_edge(START, "profile_data")
    graph.add_edge("profile_data", "generate")
    graph.add_edge("generate", "hitl")

    graph.add_conditional_edges(
        "hitl",
        approval_router,
        ["apply", "regenerate", "hitl", END]
    )

    graph.add_edge("regenerate", "hitl")
    graph.add_edge("apply", END)

    return graph.compile()
