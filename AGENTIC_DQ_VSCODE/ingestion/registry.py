# ingestion/registry.py - Local table registry
import os
import json
REGISTRY_FILE = "data/registry.json"
def register_table(table_name: str, file_path: str):
    """Register a bronze table mapping to file."""
    registry = load_registry()
    registry[table_name] = file_path
    with open(REGISTRY_FILE, "w") as f:
        json.dump(registry, f, indent=2)
def load_registry() -> dict:
    if os.path.exists(REGISTRY_FILE):
        with open(REGISTRY_FILE, "r") as f:
            return json.load(f)
    return {}
def get_table_path(table_name: str, layer: str = "bronze") -> str:
    registry = load_registry()
    base = {"bronze": BRONZE_DIR, "silver": SILVER_DIR, "quarantine": QUARANTINE_DIR}[layer]
    file_name = registry.get(table_name, f"{table_name}.csv")
    return os.path.join(base, file_name)