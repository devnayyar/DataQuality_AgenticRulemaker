# llm/feedback_loop.py
from llm.gemini_client import model
def incorporate_feedback(rules: list, feedback: str) -> list:
    """Regenerate rules with feedback."""
    prompt = f"""
    Existing rules: {rules}
    Feedback: {feedback}
    Improve and return updated JSON list of Pandas expressions.
    """
    response = model.generate_content(prompt)
    try:
        import json
        return json.loads(response.text)
    except:
        return rules  # Fallback