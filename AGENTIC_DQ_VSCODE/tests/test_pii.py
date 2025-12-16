# tests/test_pii.py
from profiling.pii_detector import detect_pii
def test_detect_email():
    sample = [{"email": "test@example.com"}]
    pii = detect_pii(sample)
    assert "email" in pii