# llm/rule_generator.py
import json
import logging
from typing import List, Optional
from pydantic import BaseModel, Field, validator
from llm.gemini_client import model
from profiling.pii_transformer import generate_pii_transformation_rules

logger = logging.getLogger(__name__)


# -------------------------------------------------
# Pydantic Models
# -------------------------------------------------
class Rule(BaseModel):
    """Represents a single data quality rule."""
    expression: str = Field(..., description="Pandas expression for rule evaluation")
    category: str = Field(..., description="Type of rule (pii, null_check, format, etc.)")
    description: str = Field(default="", description="Human-readable rule description")
    severity: str = Field(default="warning", description="Rule severity: critical, warning, info")
    
    @validator('severity')
    def validate_severity(cls, v):
        if v not in ["critical", "warning", "info"]:
            raise ValueError(f"Severity must be one of: critical, warning, info. Got: {v}")
        return v


class RuleSet(BaseModel):
    """Represents a collection of generated rules."""
    rules: List[Rule] = Field(..., description="List of data quality rules")
    total_rules: int = Field(..., description="Total number of rules generated")
    model_name: str = Field(default="gemini-2.5-flash", description="LLM model used")
    
    def to_expressions(self) -> List[str]:
        """Convert rules to simple expressions for backward compatibility."""
        return [rule.expression for rule in self.rules]


# -------------------------------------------------
# Rule Generators
# -------------------------------------------------
def generate_pii_rules(pii_fields: list, pii_types: dict = None) -> List[str]:
    """
    Dynamically generate PII transformation rules using Gemini.
    
    Creates mask/hash/removal logic specific to each PII field type (email, phone, SSN, name, etc).
    Rules are generated based on PII categories detected by Presidio, not hardcoded.
    
    Args:
        pii_fields: List of PII column names to transform
        pii_types: Dict mapping field names to PII entity types (e.g., {'email': 'EMAIL_ADDRESS', 'ssn': 'US_SSN'})
    
    Returns:
        List of transformation rule expressions (df['col'].apply(...))
    """
    if not pii_fields:
        logger.warning("No PII fields detected")
        return []
    
    pii_types = pii_types or {}
    
    prompt = f"""
    You are a data privacy expert. Generate Pandas transformation rules to protect PII fields.
    
    For each PII field below, create a transformation rule that masks or hashes the sensitive data.
    
    **PII Fields to Transform:**
    {json.dumps({field: pii_types.get(field, 'UNKNOWN') for field in pii_fields})}
    
    **Transformation Strategy:**
    - EMAIL: Mask with format xxx@example.com (keep domain, mask username)
    - PHONE: Keep last 4 digits, mask rest as XXX-XXX-4567
    - SSN/TAX_ID: Keep last 4 digits, mask rest as XXX-XX-6789
    - CREDIT_CARD: Keep last 4 digits, format as XXXX-XXXX-XXXX-1234
    - PERSON: Hash with SHA-256, take first 16 chars (e.g. a1b2c3d4e5f6g7h8)
    - LOCATION/ADDRESS: Replace with [REMOVED]
    - DATE_OF_BIRTH: Replace with 1900-01-01
    - GENERIC: Hash with SHA-256, first 16 chars
    
    **Rules must:**
    1. Use only Pandas string methods (.str.slice, .apply, etc.) or hashlib
    2. Return valid Pandas expressions with .apply() for custom logic
    3. Preserve data type consistency
    4. Handle null/NaN values gracefully
    
    **Example outputs:**
    - "df['email'].apply(lambda x: 'xxx@example.com' if pd.isna(x) else x.split('@')[0][:3] + '@example.com')"
    - "df['phone'].apply(lambda x: 'XXX-XXX-' + str(x)[-4:] if pd.notna(x) else x)"
    - "df['ssn'].apply(lambda x: 'XXX-XX-' + str(x)[-4:] if pd.notna(x) else x)"
    
    Return ONLY valid JSON (no markdown, no code blocks):
    {{
        "rules": [
            {{"expression": "df['email'].apply(lambda x: ...)", "field": "email", "pii_type": "EMAIL_ADDRESS", "strategy": "mask"}},
            {{"expression": "df['phone'].apply(lambda x: ...)", "field": "phone", "pii_type": "PHONE_NUMBER", "strategy": "mask"}}
        ],
        "total_rules": 2,
        "model_name": "gemini-2.5-flash"
    }}
    """
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        logger.debug(f"Gemini PII rules response (first 300 chars): {response_text[:300]}")
        
        # Extract JSON from response
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            response_text = response_text[json_start:json_end].strip()
        elif "```" in response_text:
            json_start = response_text.find("```") + 3
            json_end = response_text.find("```", json_start)
            if json_end > json_start:
                response_text = response_text[json_start:json_end].strip()
        
        # Find first valid JSON object
        json_start = response_text.find('{')
        json_end = response_text.rfind('}')
        if json_start >= 0 and json_end > json_start:
            response_text = response_text[json_start:json_end+1]
        
        parsed = json.loads(response_text)
        
        # Extract just the expressions from the rules
        pii_rules = [rule.get("expression") for rule in parsed.get("rules", [])]
        logger.info(f"Generated {len(pii_rules)} dynamic PII transformation rule(s)")
        
        return pii_rules
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse PII rules JSON: {e}. Raw response: {response_text[:300]}")
        logger.warning("Falling back to static PII transformation rules")
        # Fallback to static rules if LLM fails
        transformation_rules = generate_pii_transformation_rules(pii_fields)
        return transformation_rules
    except Exception as e:
        logger.error(f"Error generating PII rules: {e}")
        logger.warning("Falling back to static PII transformation rules")
        # Fallback to static rules if LLM fails
        transformation_rules = generate_pii_transformation_rules(pii_fields)
        return transformation_rules


def generate_general_rules(schema: str, profile: dict, pii_fields: list = None, past_rules: list = None) -> List[str]:
    """
    Generate comprehensive DQ rules using Gemini.
    
    Rules include:
    - Null/Missing value checks
    - Data type validation
    - Range/length validation
    - Pattern matching (email, phone formats)
    - Consistency checks (logical relationships)
    - Uniqueness constraints
    - Statistical outlier detection
    
    Args:
        schema: Column data types
        profile: Data profiling statistics
        pii_fields: List of PII columns to exclude from validation rules (they will be transformed)
        past_rules: Previously approved rules to avoid duplication
    """
    pii_fields = pii_fields or []
    past_rules = past_rules or []
    
    # Build list of non-PII columns
    non_pii_columns = [col for col in profile.get("column_stats", {}).keys() if col not in pii_fields]
    
    prompt = f"""
    Generate 6-10 comprehensive Pandas data quality rules for this dataset.
    
    CRITICAL: Do NOT create validation rules for these PII columns (they will be transformed/masked):
    {pii_fields}
    
    CREATE VALIDATION RULES FOR ALL OTHER COLUMNS:
    {non_pii_columns}
    
    Generate diverse rules including:
    1. **Null/Missing Checks**: Ensure critical fields are not null
    2. **Data Type Validation**: Verify columns have expected types
    3. **Range Validation**: For numeric fields, check min/max ranges
    4. **String Length**: For text fields, check reasonable string lengths
    5. **Pattern Matching**: Validate format (e.g., email pattern, numeric format)
    6. **Consistency Rules**: Check logical relationships between columns (e.g., if order_status='cancelled', amount should be null)
    7. **Uniqueness**: Check if certain fields should be unique
    8. **Statistical**: Flag extreme outliers or unusual values
    9. **Domain Rules**: Validate values against expected domains (e.g., status in ['active', 'inactive'])
    
    Use ONLY these Pandas operations:
    .notnull(), .isnull(), .isin(), .str.contains(), .str.len(), .astype(), 
    pd.to_datetime(), .quantile(), .between(), .abs(), .duplicated()
    
    DO NOT use modulo (%), division, or mathematical operators on string columns.
    DO NOT validate date format on PII fields - they will be masked.
    
    Schema: {schema}
    Profile: {profile}
    Previously approved rules: {past_rules}
    
    REQUIREMENTS:
    - Each rule must be a valid Pandas boolean expression
    - Rules must work on transformed (non-PII) data
    - Avoid duplicate logic from past rules
    - Return ONLY valid JSON (no markdown, no code blocks):
    {{
        "rules": [
            {{"expression": "df['column'].notnull()", "category": "null_check", "description": "Check not null", "severity": "critical"}},
            {{"expression": "(df['amount'] >= 0) & (df['amount'] <= 10000)", "category": "range", "description": "Amount within valid range", "severity": "warning"}}
        ],
        "total_rules": 6,
        "model_name": "gemini-2.5-flash"
    }}
    
    EXAMPLES:
    - "df['amount'].notnull() & (df['amount'] > 0)"
    - "df['status'].isin(['active', 'inactive', 'pending'])"
    - "(df['qty'] > 0) == (df['amount'] > 0)" # Logical consistency
    - "df['name'].str.len() > 2"
    - "~df.duplicated(subset=['order_id'], keep=False)"
    - "df['quantity'].between(0, 1000)"
    """
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        logger.debug(f"Gemini general rules response (first 300 chars): {response_text[:300]}")
        
        # Try to extract JSON from response
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            response_text = response_text[json_start:json_end].strip()
        elif "```" in response_text:
            json_start = response_text.find("```") + 3
            json_end = response_text.find("```", json_start)
            if json_end > json_start:
                response_text = response_text[json_start:json_end].strip()
        
        # Find first valid JSON object
        json_start = response_text.find('{')
        json_end = response_text.rfind('}')
        if json_start >= 0 and json_end > json_start:
            response_text = response_text[json_start:json_end+1]
        
        parsed = json.loads(response_text)
        rule_set = RuleSet(**parsed)
        logger.info(f"Generated {rule_set.total_rules} general rules")
        return rule_set.to_expressions()
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse general rules JSON: {e}. Raw response: {response_text[:300]}")
        logger.warning("Continuing with empty general rules")
        return []
    except Exception as e:
        logger.error(f"Error generating general rules: {e}")
        logger.warning("Continuing with empty general rules")
        return []