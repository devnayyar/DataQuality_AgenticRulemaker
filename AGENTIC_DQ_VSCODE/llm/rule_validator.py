# llm/rule_validator.py
import ast
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def validate_rules(rules: list) -> Tuple[bool, Optional[str]]:
    """
    Validate Pandas rule expressions for syntax and safety.
    
    Args:
        rules: List of rule expressions (strings)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    
    # Banned dangerous functions and keywords
    banned_keywords = {
        "__import__", "exec", "compile", "open", "input",
        "globals", "locals", "vars", "getattr", "setattr", "delattr",
        "hasattr", "eval", "type", "__builtins__", "__loader__", "__spec__"
    }
    
    if not rules:
        return True, None
    
    if not isinstance(rules, list):
        return False, f"Rules must be a list, got {type(rules)}"
    
    for idx, rule in enumerate(rules):
        # Type check
        if not isinstance(rule, str):
            return False, f"Rule {idx} is not a string: {type(rule)}"
        
        if not rule.strip():
            return False, f"Rule {idx} is empty"
        
        # Check for obviously dangerous patterns
        dangerous_patterns = ["os.", "sys.", "import ", "__", "open(", "exec(", "eval("]
        for pattern in dangerous_patterns:
            if pattern in rule:
                logger.warning(f"Rule {idx} contains potentially unsafe pattern: {pattern}")
                return False, f"Rule {idx} contains unsafe pattern: {pattern}"
        
        # Try to parse as valid Python expression
        try:
            tree = ast.parse(rule, mode='eval')
        except SyntaxError as e:
            logger.error(f"Rule {idx} syntax error: {e}")
            return False, f"Rule {idx} has syntax error: {str(e)}"
        except Exception as e:
            logger.error(f"Rule {idx} parse error: {e}")
            return False, f"Rule {idx} failed to parse: {str(e)}"
        
        # Check AST for banned function calls
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in banned_keywords:
                        return False, f"Rule {idx} calls banned function: {node.func.id}"
        
        # Validation: rule should reference 'df' or column operations
        if "df[" not in rule and "df." not in rule:
            logger.warning(f"Rule {idx} doesn't reference dataframe object")
            return False, f"Rule {idx} doesn't reference dataframe: {rule}"
        
        logger.debug(f"Rule {idx} validation passed: {rule[:50]}...")
    
    logger.info(f"All {len(rules)} rules passed validation")
    return True, None