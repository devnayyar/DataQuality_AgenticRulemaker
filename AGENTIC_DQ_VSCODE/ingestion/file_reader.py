# ingestion/file_reader.py
import pandas as pd
from ingestion.registry import register_table,get_table_path
import os 

def read_bronze(table_name: str) -> pd.DataFrame:
    """Read from bronze layer."""
    path = get_table_path(table_name, "bronze")
    return pd.read_csv(path)
def ingest_file(file_path: str, table_name: str):
    """Ingest file to bronze."""
    df = pd.read_csv(file_path)  # Assume CSV; extend for others
    bronze_path = get_table_path(table_name, "bronze")
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
    df.to_csv(bronze_path, index=False)
    register_table(table_name, os.path.basename(bronze_path))