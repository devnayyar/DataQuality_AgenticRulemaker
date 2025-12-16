# tests/test_profiler.py
def test_profile_structure(df):
    from profiling.statistical_profiler import generate_profile
    profile = generate_profile(df)
    assert "row_count" in profile
    assert profile["row_count"] == 1