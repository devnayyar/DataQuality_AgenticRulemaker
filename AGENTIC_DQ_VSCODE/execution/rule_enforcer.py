# execution/rule_enforcer.py
import logging
import os
import tempfile
from typing import Dict, List, Any, Optional

import pandas as pd

from config.settings import SILVER_DIR, QUARANTINE_DIR
from llm.rule_validator import validate_rules
from profiling.pii_transformer import apply_pii_transformations

logger = logging.getLogger(__name__)


def apply_rules(df: pd.DataFrame, table_name: str, rules: List[str]) -> Dict[str, Any]:
    """
    Apply data quality rules to a dataframe and partition into Silver/Quarantine.
    
    Args:
        df: Input dataframe to process
        table_name: Name of the table for file naming
        rules: List of Pandas boolean expressions
        
    Returns:
        Dictionary with metrics: total, passed, failed, pass_rate, failed_rules
    """
    
    # Default response for empty rules
    if not rules:
        logger.warning("No rules provided for table %s", table_name)
        return {
            "total": len(df),
            "passed": 0,
            "failed": len(df),
            "pass_rate": 0.0,
            "failed_rules": [],
            "warning": "No rules to apply"
        }
    
    # Validate rules before applying (defensive)
    is_valid, error_msg = validate_rules(rules)
    if not is_valid:
        logger.error("Rule validation failed for %s: %s", table_name, error_msg)
        return {
            "total": len(df),
            "passed": 0,
            "failed": len(df),
            "pass_rate": 0.0,
            "failed_rules": rules,
            "error": f"Rule validation failed: {error_msg}"
        }
    
    # Apply rules individually with AND logic
    try:
        mask = pd.Series([True] * len(df), index=df.index)
        failed_rules = []
        
        for idx, rule in enumerate(rules):
            try:
                # Safely evaluate each rule
                rule_mask = eval(rule, {"df": df, "pd": pd})
                
                # Ensure result is boolean Series
                if not isinstance(rule_mask, (pd.Series, bool)):
                    logger.warning("Rule %d returned non-boolean: %s", idx, type(rule_mask))
                    failed_rules.append({"index": idx, "rule": rule, "error": "Did not return boolean"})
                    continue
                
                mask = mask & rule_mask
                logger.debug("Rule %d applied successfully: %s", idx, rule[:50])
                
            except Exception as e:
                logger.warning("Rule %d failed (skipping): %s. Error: %s", idx, rule[:50], str(e))
                failed_rules.append({"index": idx, "rule": rule, "error": str(e)})
                continue
        
        # Partition data
        clean = df[mask]
        bad = df[~df.index.isin(clean.index)]
        
        logger.info("Rule evaluation complete for %s: %d passed, %d failed",
                   table_name, len(clean), len(bad))
        
    except Exception as e:
        logger.error("Unexpected error during rule evaluation for %s: %s", table_name, str(e))
        return {
            "total": len(df),
            "passed": 0,
            "failed": len(df),
            "pass_rate": 0.0,
            "failed_rules": rules,
            "error": f"Rule evaluation error: {str(e)}"
        }
    
    # Save data with atomic writes
    try:
        _save_partitions(clean, bad, table_name)
        logger.info("Saved partitions for %s to Silver and Quarantine", table_name)
    except Exception as e:
        logger.error("Failed to save partitions for %s: %s", table_name, str(e))
        return {
            "total": len(df),
            "passed": len(clean),
            "failed": len(bad),
            "pass_rate": len(clean) / len(df) if len(df) > 0 else 0.0,
            "failed_rules": failed_rules,
            "error": f"Failed to save partitions: {str(e)}"
        }
    
    return {
        "total": len(df),
        "passed": len(clean),
        "failed": len(bad),
        "pass_rate": len(clean) / len(df) if len(df) > 0 else 0.0,
        "failed_rules": failed_rules,
        "rules_applied": len(rules) - len(failed_rules)
    }


def _save_partitions(clean: pd.DataFrame, bad: pd.DataFrame, table_name: str) -> None:
    """
    Save clean and quarantined data to CSV files atomically.
    
    Args:
        clean: DataFrame with passing records
        bad: DataFrame with failing records
        table_name: Table name for file naming
        
    Raises:
        Exception: If save operations fail
    """
    # Create directories
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    
    silver_path = os.path.join(SILVER_DIR, f"{table_name}.csv")
    quarantine_path = os.path.join(QUARANTINE_DIR, f"{table_name}_quarantine.csv")
    
    try:
        # Save clean data atomically
        _atomic_save_csv(clean, silver_path)
        logger.debug("Saved %d clean records to %s", len(clean), silver_path)
        
        # Save bad data atomically
        _atomic_save_csv(bad, quarantine_path)
        logger.debug("Saved %d quarantined records to %s", len(bad), quarantine_path)
        
    except Exception as e:
        logger.error("Error saving partitions: %s", str(e))
        raise


def _atomic_save_csv(df: pd.DataFrame, filepath: str) -> None:
    """
    Save dataframe to CSV atomically using temp file.
    
    Args:
        df: DataFrame to save
        filepath: Target file path
        
    Raises:
        Exception: If save fails
    """
    temp_dir = os.path.dirname(filepath) or '.'
    
    try:
        # Write to temp file first
        with tempfile.NamedTemporaryFile(
            mode='w',
            dir=temp_dir,
            delete=False,
            suffix='.tmp',
            encoding='utf-8'
        ) as tmp:
            df.to_csv(tmp, index=False)
            tmp_path = tmp.name
        
        # Atomic rename
        os.replace(tmp_path, filepath)
        
    except Exception as e:
        # Clean up temp file if it exists
        try:
            if 'tmp_path' in locals():
                os.unlink(tmp_path)
        except:
            pass


def apply_rules_with_pii_transformation(
    df: pd.DataFrame,
    table_name: str,
    pii_fields: List[str],
    pii_rules: List[str],
    general_rules: List[str]
) -> Dict[str, Any]:
    """
    Apply PII transformations followed by general quality rules.
    
    Workflow:
    1. Apply PII transformation rules (mask/hash/remove sensitive data)
    2. Apply general DQ rules (validate quality)
    3. Partition clean data → Silver, failed data → Quarantine
    
    Args:
        df: Input dataframe
        table_name: Name of the table for file naming
        pii_fields: List of PII column names
        pii_rules: List of PII transformation rules (exec-based)
        general_rules: List of general validation rules (boolean expressions)
        
    Returns:
        Dictionary with metrics: total, passed, failed, pass_rate, failed_rules
    """
    
    logger.info(f"Processing {table_name} with {len(pii_fields)} PII field(s)")
    
    # STEP 1: Apply PII Transformations
    df_transformed = df.copy()
    
    if pii_fields:
        logger.info(f"Applying PII transformations to {len(pii_fields)} field(s)")
        df_transformed = apply_pii_transformations(df_transformed, pii_fields)
        logger.info("✅ PII transformations complete")
    
    # STEP 2: Apply General DQ Rules
    if not general_rules:
        logger.warning(f"No general rules provided for {table_name}")
        # Save all transformed data to Silver (no quality validation rules)
        try:
            _save_partitions(df_transformed, pd.DataFrame(), table_name)
            logger.info(f"✅ Saved {len(df_transformed)} record(s) to Silver (PII transformed, no validation rules)")
            return {
                "total": len(df_transformed),
                "passed": len(df_transformed),
                "failed": 0,
                "pass_rate": 1.0,
                "failed_rules": [],
                "message": "No general rules to apply (PII transformed only)"
            }
        except Exception as e:
            logger.error(f"Failed to save partitions: {str(e)}")
            return {
                "total": len(df_transformed),
                "passed": 0,
                "failed": len(df_transformed),
                "pass_rate": 0.0,
                "failed_rules": [],
                "error": f"Failed to save partitions: {str(e)}"
            }
    
    # Validate general rules
    is_valid, error_msg = validate_rules(general_rules)
    if not is_valid:
        logger.error(f"General rule validation failed: {error_msg}")
        return {
            "total": len(df_transformed),
            "passed": 0,
            "failed": len(df_transformed),
            "pass_rate": 0.0,
            "failed_rules": general_rules,
            "error": f"Rule validation failed: {error_msg}"
        }
    
    # Apply general rules with AND logic
    try:
        mask = pd.Series([True] * len(df_transformed), index=df_transformed.index)
        failed_rules = []
        
        logger.info(f"Evaluating {len(general_rules)} general rule(s)")
        
        for idx, rule in enumerate(general_rules):
            try:
                # Safely evaluate each rule
                rule_mask = eval(rule, {"df": df_transformed, "pd": pd})
                
                # Ensure result is boolean Series
                if not isinstance(rule_mask, (pd.Series, bool)):
                    logger.warning(f"Rule {idx} returned non-boolean: {type(rule_mask)}")
                    failed_rules.append({"index": idx, "rule": rule, "error": "Did not return boolean"})
                    continue
                
                mask = mask & rule_mask
                logger.debug(f"Rule {idx} applied: {rule[:60]}")
                
            except Exception as e:
                logger.warning(f"Rule {idx} failed: {rule[:60]}. Error: {str(e)}")
                failed_rules.append({"index": idx, "rule": rule, "error": str(e)})
                continue
        
        # Partition data
        clean = df_transformed[mask]
        bad = df_transformed[~df_transformed.index.isin(clean.index)]
        
        passed_count = len(clean)
        failed_count = len(bad)
        pass_rate = (passed_count / len(df_transformed)) if len(df_transformed) > 0 else 0.0
        
        logger.info(f"Rule evaluation complete: {passed_count} passed, {failed_count} failed (pass_rate: {pass_rate:.2%})")
        
    except Exception as e:
        logger.error(f"Unexpected error during rule evaluation: {str(e)}")
        return {
            "total": len(df_transformed),
            "passed": 0,
            "failed": len(df_transformed),
            "pass_rate": 0.0,
            "failed_rules": general_rules,
            "error": f"Rule evaluation error: {str(e)}"
        }
    
    # STEP 3: Save partitions
    try:
        _save_partitions(clean, bad, table_name)
        logger.info(f"✅ Saved {passed_count} clean record(s) to Silver, {failed_count} to Quarantine")
    except Exception as e:
        logger.error(f"Failed to save partitions: {str(e)}")
        return {
            "total": len(df_transformed),
            "passed": passed_count,
            "failed": failed_count,
            "pass_rate": pass_rate,
            "failed_rules": failed_rules,
            "error": f"Failed to save partitions: {str(e)}"
        }
    
    return {
        "total": len(df_transformed),
        "passed": passed_count,
        "failed": failed_count,
        "pass_rate": pass_rate,
        "failed_rules": failed_rules,
        "message": "Processing complete with PII transformations applied"
    }
    raise