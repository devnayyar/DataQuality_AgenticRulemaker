# tests/conftest.py
import pytest
import pandas as pd
@pytest.fixture(scope="session")
def df():
    return pd.DataFrame({"id": [1]})