# profiling/pii_transformer.py
"""PII data transformation and masking utilities."""

import pandas as pd
import hashlib
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


def mask_email(email: str) -> str:
    """
    Mask email address.
    Example: john.doe@gmail.com → xxx@example.com
    """
    if pd.isna(email) or not isinstance(email, str):
        return email
    return "xxx@example.com"


def mask_phone(phone: str) -> str:
    """
    Mask phone number, keeping last 4 digits.
    Example: 555-123-4567 → XXX-XXX-4567
    """
    if pd.isna(phone) or not isinstance(phone, str):
        return phone
    
    # Remove all non-digits
    digits = ''.join(filter(str.isdigit, str(phone)))
    
    if len(digits) < 4:
        return "XXX-XXX-XXXX"
    
    # Keep only last 4 digits
    last_four = digits[-4:]
    return f"XXX-XXX-{last_four}"


def mask_ssn(ssn: str) -> str:
    """
    Mask SSN/ID, keeping last 4 digits.
    Example: 123-45-6789 → XXX-XX-6789
    """
    if pd.isna(ssn) or not isinstance(ssn, str):
        return ssn
    
    # Remove all non-digits
    digits = ''.join(filter(str.isdigit, str(ssn)))
    
    if len(digits) < 4:
        return "XXX-XX-XXXX"
    
    # Keep only last 4 digits
    last_four = digits[-4:]
    return f"XXX-XX-{last_four}"


def mask_credit_card(cc: str) -> str:
    """
    Mask credit card, keeping last 4 digits.
    Example: 4532-1234-5678-9012 → XXXX-XXXX-XXXX-9012
    """
    if pd.isna(cc) or not isinstance(cc, str):
        return cc
    
    # Remove all non-digits
    digits = ''.join(filter(str.isdigit, str(cc)))
    
    if len(digits) < 4:
        return "XXXX-XXXX-XXXX-XXXX"
    
    # Keep only last 4 digits
    last_four = digits[-4:]
    return f"XXXX-XXXX-XXXX-{last_four}"


def hash_name(name: str) -> str:
    """
    Hash name using SHA-256.
    Example: John Doe → a1b2c3d4e5f6... (truncated to 16 chars)
    """
    if pd.isna(name) or not isinstance(name, str):
        return name
    
    hash_obj = hashlib.sha256(str(name).encode())
    return hash_obj.hexdigest()[:16]


def remove_address(address: str) -> str:
    """
    Remove address completely.
    Example: 123 Main St, City, ST 12345 → [REMOVED]
    """
    if pd.isna(address) or not isinstance(address, str):
        return address
    return "[REMOVED]"


def generate_pii_transformation_rules(pii_fields: List[str]) -> List[str]:
    """
    Generate transformation rules for detected PII fields.
    
    Args:
        pii_fields: List of column names identified as PII
        
    Returns:
        List of transformation rule expressions
    """
    rules = []
    
    for field in pii_fields:
        field_lower = field.lower()
        
        # Email transformation
        if 'email' in field_lower or 'mail' in field_lower:
            rules.append(f"df['{field}'] = df['{field}'].apply(lambda x: mask_email(x))")
        
        # Phone transformation
        elif 'phone' in field_lower or 'tel' in field_lower or 'mobile' in field_lower:
            rules.append(f"df['{field}'] = df['{field}'].apply(lambda x: mask_phone(x))")
        
        # SSN/ID transformation
        elif 'ssn' in field_lower or 'social' in field_lower or 'id_number' in field_lower:
            rules.append(f"df['{field}'] = df['{field}'].apply(lambda x: mask_ssn(x))")
        
        # Credit card transformation
        elif 'credit' in field_lower or 'cc_' in field_lower or 'card' in field_lower:
            rules.append(f"df['{field}'] = df['{field}'].apply(lambda x: mask_credit_card(x))")
        
        # Name transformation (hash)
        elif 'name' in field_lower or 'fname' in field_lower or 'lname' in field_lower or 'first' in field_lower or 'last' in field_lower:
            rules.append(f"df['{field}'] = df['{field}'].apply(lambda x: hash_name(x))")
        
        # Address transformation (remove)
        elif 'address' in field_lower or 'street' in field_lower or 'location' in field_lower:
            rules.append(f"df['{field}'] = df['{field}'].apply(lambda x: remove_address(x))")
        
        # Default: hash for unknown PII types
        else:
            rules.append(f"df['{field}'] = df['{field}'].apply(lambda x: hash_name(x))")
    
    return rules


def apply_pii_transformations(df: pd.DataFrame, pii_fields: List[str]) -> pd.DataFrame:
    """
    Apply PII transformations to dataframe.
    
    Args:
        df: Input dataframe
        pii_fields: List of PII column names
        
    Returns:
        Dataframe with PII fields masked/transformed
    """
    df_copy = df.copy()
    
    try:
        for field in pii_fields:
            if field not in df_copy.columns:
                logger.warning(f"PII field '{field}' not found in dataframe")
                continue
            
            field_lower = field.lower()
            
            # Apply appropriate transformation based on field type
            if 'email' in field_lower or 'mail' in field_lower:
                df_copy[field] = df_copy[field].apply(mask_email)
                logger.debug(f"Masked email field: {field}")
            
            elif 'phone' in field_lower or 'tel' in field_lower or 'mobile' in field_lower:
                df_copy[field] = df_copy[field].apply(mask_phone)
                logger.debug(f"Masked phone field: {field}")
            
            elif 'ssn' in field_lower or 'social' in field_lower or 'id_number' in field_lower:
                df_copy[field] = df_copy[field].apply(mask_ssn)
                logger.debug(f"Masked SSN/ID field: {field}")
            
            elif 'credit' in field_lower or 'cc_' in field_lower or 'card' in field_lower:
                df_copy[field] = df_copy[field].apply(mask_credit_card)
                logger.debug(f"Masked credit card field: {field}")
            
            elif 'name' in field_lower or 'fname' in field_lower or 'lname' in field_lower or 'first' in field_lower or 'last' in field_lower:
                df_copy[field] = df_copy[field].apply(hash_name)
                logger.debug(f"Hashed name field: {field}")
            
            elif 'address' in field_lower or 'street' in field_lower or 'location' in field_lower:
                df_copy[field] = df_copy[field].apply(remove_address)
                logger.debug(f"Removed address field: {field}")
            
            else:
                # Default: hash unknown PII
                df_copy[field] = df_copy[field].apply(hash_name)
                logger.debug(f"Hashed unknown PII field: {field}")
        
        logger.info(f"PII transformations applied to {len(pii_fields)} field(s)")
        return df_copy
    
    except Exception as e:
        logger.error(f"Error applying PII transformations: {e}")
        return df_copy
