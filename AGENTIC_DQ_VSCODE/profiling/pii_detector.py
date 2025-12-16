# profiling/pii_detector.py
import logging
from typing import List, Dict, Any, Set
from presidio_analyzer import AnalyzerEngine

logger = logging.getLogger(__name__)

# Initialize analyzer once (singleton pattern for performance)
_analyzer = None

def _get_analyzer() -> AnalyzerEngine:
    """Get or create Presidio AnalyzerEngine (lazy initialization)."""
    global _analyzer
    if _analyzer is None:
        try:
            _analyzer = AnalyzerEngine()
            logger.debug("Initialized Presidio AnalyzerEngine")
        except Exception as e:
            logger.error(f"Failed to initialize Presidio: {e}")
            raise
    return _analyzer


def detect_pii(
    sample_rows: List[Dict[str, Any]], 
    min_confidence: float = 0.5,
    max_sample_size: int = 10
) -> List[str]:
    """
    Detect PII (Personally Identifiable Information) fields using Presidio.
    
    Args:
        sample_rows: List of sample rows (dictionaries) from the dataset
        min_confidence: Minimum confidence threshold for PII detection (0-1)
        max_sample_size: Maximum number of rows to sample for analysis
        
    Returns:
        List of column names identified as containing PII
        
    Example:
        >>> rows = [{"email": "user@example.com", "name": "John", "age": 30}]
        >>> detect_pii(rows)
        ['email', 'name']
    """
    
    if not sample_rows:
        logger.warning("Empty sample rows provided to detect_pii")
        return []
    
    if not isinstance(sample_rows, list):
        logger.error(f"Expected list, got {type(sample_rows)}")
        return []
    
    if min_confidence < 0 or min_confidence > 1:
        logger.warning(f"Invalid confidence threshold {min_confidence}, using 0.5")
        min_confidence = 0.5
    
    try:
        analyzer = _get_analyzer()
    except Exception as e:
        logger.error(f"Cannot analyze PII without Presidio: {e}")
        return []
    
    # PII entities to detect
    pii_entities = [
        "EMAIL_ADDRESS",
        "PHONE_NUMBER", 
        "PERSON",
        "CREDIT_CARD",
        "US_SSN",
        "IP_ADDRESS",
        "IBAN_CODE",
        "DATE_TIME"  # Dates can be sensitive in some contexts
    ]
    
    pii_columns: Set[str] = set()
    sample_size = min(len(sample_rows), max_sample_size)
    analysis_count = 0
    error_count = 0
    
    logger.info(f"Scanning {sample_size} sample rows for PII...")
    
    try:
        for idx, row in enumerate(sample_rows[:sample_size]):
            if not isinstance(row, dict):
                logger.warning(f"Row {idx} is not a dictionary, skipping")
                continue
            
            for column_name, cell_value in row.items():
                # Only analyze string values with meaningful length
                if not isinstance(cell_value, str) or len(str(cell_value).strip()) <= 3:
                    continue
                
                try:
                    # Analyze cell for PII
                    analysis_results = analyzer.analyze(
                        text=str(cell_value),
                        language="en",
                        entities=pii_entities
                    )
                    
                    # Check if any PII detected above confidence threshold
                    for result in analysis_results:
                        if result.score >= min_confidence:
                            pii_columns.add(column_name)
                            logger.debug(
                                f"PII detected in column '{column_name}': "
                                f"{result.entity_type} (confidence: {result.score:.2f})"
                            )
                            break  # Move to next cell once PII found
                    
                    analysis_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error analyzing cell [{idx}, {column_name}]: {e}")
                    error_count += 1
                    continue
        
        logger.info(
            f"PII detection complete: {analysis_count} cells analyzed, "
            f"{error_count} errors, {len(pii_columns)} PII columns detected"
        )
        
        if pii_columns:
            logger.warning(f"PII fields detected: {sorted(pii_columns)}")
        else:
            logger.info("No PII fields detected")
        
        return sorted(list(pii_columns))
        
    except Exception as e:
        logger.error(f"Unexpected error during PII detection: {e}")
        return []


def detect_pii_with_types(
    sample_rows: List[Dict[str, Any]], 
    min_confidence: float = 0.5,
    max_sample_size: int = 10
) -> tuple[List[str], Dict[str, str]]:
    """
    Detect PII fields and return both field names and their entity types.
    
    Args:
        sample_rows: List of sample rows (dictionaries) from the dataset
        min_confidence: Minimum confidence threshold for PII detection (0-1)
        max_sample_size: Maximum number of rows to sample for analysis
        
    Returns:
        Tuple of (pii_field_list, pii_type_mapping)
        - pii_field_list: List of column names with PII
        - pii_type_mapping: Dict mapping field name to primary PII entity type
        
    Example:
        >>> rows = [{"email": "user@example.com", "name": "John", "age": 30}]
        >>> detect_pii_with_types(rows)
        (['email', 'name'], {'email': 'EMAIL_ADDRESS', 'name': 'PERSON'})
    """
    
    if not sample_rows:
        logger.warning("Empty sample rows provided to detect_pii_with_types")
        return [], {}
    
    if not isinstance(sample_rows, list):
        logger.error(f"Expected list, got {type(sample_rows)}")
        return [], {}
    
    if min_confidence < 0 or min_confidence > 1:
        logger.warning(f"Invalid confidence threshold {min_confidence}, using 0.5")
        min_confidence = 0.5
    
    try:
        analyzer = _get_analyzer()
    except Exception as e:
        logger.error(f"Cannot analyze PII without Presidio: {e}")
        return [], {}
    
    # PII entities to detect
    pii_entities = [
        "EMAIL_ADDRESS",
        "PHONE_NUMBER", 
        "PERSON",
        "CREDIT_CARD",
        "US_SSN",
        "IP_ADDRESS",
        "IBAN_CODE",
        "DATE_TIME"
    ]
    
    pii_columns: Set[str] = set()
    pii_types: Dict[str, str] = {}  # Map column -> primary PII type
    sample_size = min(len(sample_rows), max_sample_size)
    
    logger.info(f"Scanning {sample_size} sample rows for PII with types...")
    
    try:
        for idx, row in enumerate(sample_rows[:sample_size]):
            if not isinstance(row, dict):
                continue
            
            for column_name, cell_value in row.items():
                # Skip if already identified and we have the type
                if column_name in pii_types:
                    continue
                
                # Only analyze string values with meaningful length
                if not isinstance(cell_value, str) or len(str(cell_value).strip()) <= 3:
                    continue
                
                try:
                    analysis_results = analyzer.analyze(
                        text=str(cell_value),
                        language="en",
                        entities=pii_entities
                    )
                    
                    # Get the highest confidence PII entity for this field
                    best_result = None
                    for result in analysis_results:
                        if result.score >= min_confidence:
                            if best_result is None or result.score > best_result.score:
                                best_result = result
                    
                    if best_result:
                        pii_columns.add(column_name)
                        pii_types[column_name] = best_result.entity_type
                        logger.debug(
                            f"PII detected in column '{column_name}': "
                            f"{best_result.entity_type} (confidence: {best_result.score:.2f})"
                        )
                    
                except Exception as e:
                    logger.warning(f"Error analyzing cell [{idx}, {column_name}]: {e}")
                    continue
        
        logger.info(
            f"PII detection complete: {len(pii_columns)} PII columns with types: {pii_types}"
        )
        
        if pii_columns:
            logger.warning(f"PII fields detected: {sorted(pii_columns)} with types: {pii_types}")
        else:
            logger.info("No PII fields detected")
        
        return sorted(list(pii_columns)), pii_types
        
    except Exception as e:
        logger.error(f"Unexpected error during PII detection with types: {e}")
        return [], {}