# hitl/controller.py
import uuid
import json
import os
import logging
import tempfile
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

from memory.faiss_store import rag

logger = logging.getLogger(__name__)

# --- File Persistence Setup ---
REVIEW_FILE = "pending_reviews.json"


def _load_reviews() -> Dict[str, Any]:
    """
    Load reviews from the persistent JSON file.
    
    Returns:
        Dictionary of review sessions, empty dict if file doesn't exist.
    """
    if not os.path.exists(REVIEW_FILE):
        logger.debug(f"Review file {REVIEW_FILE} does not exist yet")
        return {}
    
    try:
        with open(REVIEW_FILE, 'r') as f:
            content = f.read().strip()
            if not content:
                logger.debug(f"Review file {REVIEW_FILE} is empty, starting fresh")
                return {}
            data = json.loads(content)
            logger.debug(f"Loaded {len(data)} review sessions from {REVIEW_FILE}")
            return data
    except json.JSONDecodeError as e:
        logger.debug(f"Review file corrupted or empty: {e}. Starting fresh.")
        return {}
    except IOError as e:
        logger.debug(f"Failed to read review file: {e}. Starting fresh.")
        return {}


def _save_reviews(reviews: Dict[str, Any]) -> bool:
    """
    Save reviews to persistent JSON file with atomic write.
    
    Args:
        reviews: Dictionary of review sessions
        
    Returns:
        True if successful, False otherwise
    """
    if not reviews:
        logger.warning("Attempted to save empty reviews dictionary")
        return False
    
    # Create a serializable copy
    serializable_reviews = {}
    try:
        for sid, sess in reviews.items():
            sess_copy = sess.copy()
            if 'created' in sess_copy and isinstance(sess_copy['created'], datetime):
                sess_copy['created'] = sess_copy['created'].isoformat()
            if 'reviewed' in sess_copy and isinstance(sess_copy['reviewed'], datetime):
                sess_copy['reviewed'] = sess_copy['reviewed'].isoformat()
            serializable_reviews[sid] = sess_copy
    except Exception as e:
        logger.error(f"Error serializing reviews: {e}")
        return False
    
    # Atomic write using temp file
    try:
        # Create temp file in same directory to ensure same filesystem
        temp_dir = os.path.dirname(REVIEW_FILE) or '.'
        with tempfile.NamedTemporaryFile(
            mode='w', 
            dir=temp_dir, 
            delete=False, 
            suffix='.tmp',
            encoding='utf-8'
        ) as tmp:
            json.dump(serializable_reviews, tmp, indent=4, default=str)
            tmp_path = tmp.name
        
        # Atomic rename
        os.replace(tmp_path, REVIEW_FILE)
        logger.debug(f"Successfully saved {len(serializable_reviews)} reviews to {REVIEW_FILE}")
        return True
    except Exception as e:
        logger.error(f"Failed to save reviews: {e}")
        # Clean up temp file if it exists
        try:
            if 'tmp_path' in locals():
                os.unlink(tmp_path)
        except:
            pass
        return False


# Load reviews on module import
_pending_reviews = _load_reviews()


# --- Validation Functions ---
def _validate_review_input(
    table_name: str, 
    rules: List[str], 
    profile: Dict[str, Any], 
    sample: List[Dict[str, Any]]
) -> Optional[str]:
    """
    Validate inputs for review creation.
    
    Args:
        table_name: Name of the table
        rules: List of rule expressions
        profile: Data profile
        sample: Sample data rows
        
    Returns:
        Error message if validation fails, None if valid
    """
    if not isinstance(table_name, str) or not table_name.strip():
        return "table_name must be a non-empty string"
    
    if not isinstance(rules, list) or not rules:
        return "rules must be a non-empty list"
    
    if not all(isinstance(r, str) for r in rules):
        return "All rules must be strings"
    
    if not isinstance(profile, dict):
        return "profile must be a dictionary"
    
    if not isinstance(sample, list):
        return "sample must be a list"
    
    return None


# --- Controller Functions ---

def create_review(
    table_name: str, 
    rules: List[str], 
    profile: Dict[str, Any], 
    sample: List[Dict[str, Any]]
) -> Optional[str]:
    """
    Create a new review session and save it to the file.
    
    Args:
        table_name: Name of the table being reviewed
        rules: List of rule expressions
        profile: Statistical profile of the data
        sample: Sample rows from the table
        
    Returns:
        Session ID if successful, None if failed
    """
    # Validate inputs
    validation_error = _validate_review_input(table_name, rules, profile, sample)
    if validation_error:
        logger.error(f"Invalid review input: {validation_error}")
        return None
    
    global _pending_reviews
    
    try:
        sid = str(uuid.uuid4())
        _pending_reviews[sid] = {
            "table": table_name,
            "rules": rules,
            "profile": profile,
            "sample": sample,
            "status": "pending",
            "created": datetime.now().isoformat()
        }
        
        if _save_reviews(_pending_reviews):
            logger.info(f"Created review session {sid} for table {table_name}")
            return sid
        else:
            logger.error(f"Failed to save review session {sid}")
            # Rollback
            del _pending_reviews[sid]
            return None
            
    except Exception as e:
        logger.error(f"Error creating review: {e}")
        return None


def submit_review(
    sid: str, 
    approved: bool, 
    edited_rules: Optional[List[str]] = None, 
    feedback: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Submit a review decision and update session state.
    
    Args:
        sid: Session ID
        approved: Whether rules were approved
        edited_rules: Rules after editing (optional)
        feedback: User feedback (optional)
        
    Returns:
        Updated session dict if successful, None otherwise
    """
    # Validate session ID
    if not isinstance(sid, str) or not sid.strip():
        logger.error("Invalid session ID")
        return None
    
    # Validate edited_rules if provided
    if edited_rules is not None and not isinstance(edited_rules, list):
        logger.error("edited_rules must be a list or None")
        return None
    
    global _pending_reviews
    
    try:
        # Reload from disk to get latest state
        _pending_reviews = _load_reviews()
        
        sess = _pending_reviews.get(sid)
        if not sess:
            logger.error(f"Review session {sid} not found")
            return None
        
        decision = "approved" if approved else "rejected"
        final_rules = edited_rules if edited_rules is not None else sess["rules"]
        
        # Store in RAG first - if this fails, don't update session
        try:
            text = f"Decision for {sess['table']}: {decision}. Feedback: {feedback}. Rules: {final_rules}"
            rag.add_feedback(text, sess['table'], decision, final_rules)
            logger.debug(f"Stored feedback in RAG for {sid}")
        except Exception as e:
            logger.error(f"Failed to store feedback in RAG: {e}. Continuing anyway.")
            # Don't fail the entire submission if RAG fails
        
        # Update session state
        sess["status"] = decision
        sess["final_rules"] = final_rules
        sess["feedback"] = feedback
        sess["reviewed"] = datetime.now().isoformat()
        
        # Save updated state
        if _save_reviews(_pending_reviews):
            logger.info(f"Review session {sid} submitted with decision: {decision}")
            return sess
        else:
            logger.error(f"Failed to save review decision for {sid}")
            return None
            
    except Exception as e:
        logger.error(f"Error submitting review {sid}: {e}")
        return None