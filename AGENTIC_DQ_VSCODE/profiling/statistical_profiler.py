# profiling/statistical_profiler.py - Pandas-based profiler (replaces Deequ)
import pandas as pd
import json
def generate_profile(df: pd.DataFrame):
    """Generate data profile using Pandas."""
    profile = {
        "row_count": len(df),
        "null_rates": df.isnull().mean().to_dict(),
        "uniqueness": (df.nunique() / len(df)).to_dict(),
        "min_max": {},
        "data_types": df.dtypes.astype(str).to_dict()
    }
    for col in df.select_dtypes(include=["number"]).columns:
        profile["min_max"][col] = {"min": df[col].min(), "max": df[col].max()}
    return profile