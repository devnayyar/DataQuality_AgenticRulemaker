# tests/test_generate_rules.py
from llm.rule_generator import generate_pii_rules
def test_generate_pii():
    rules = generate_pii_rules(["email"])
    assert isinstance(rules, list)
    assert len(rules) > 0