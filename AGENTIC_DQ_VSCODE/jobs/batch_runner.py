# jobs/batch_runner.py

import sys
import os
import logging
from itertools import islice
import json

import pandas as pd

# -------------------------------------------------
# Path setup
# -------------------------------------------------
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workflow.state_machine import build_workflow
from config.settings import BRONZE_DIR
from ingestion.registry import load_registry, register_table
from hitl.controller import _load_reviews


# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------------------------------------
# Registry Management
# -------------------------------------------------
def register_bronze_tables() -> dict:
    """
    Automatically register all CSV files in bronze directory.
    
    Returns:
        Dictionary of registered tables
    """
    logger.info("Scanning bronze directory for CSV files...")
    
    if not os.path.exists(BRONZE_DIR):
        logger.warning(f"Bronze directory does not exist: {BRONZE_DIR}")
        return {}
    
    registered_count = 0
    registry = {}
    
    for file in os.listdir(BRONZE_DIR):
        if file.endswith('.csv'):
            table_name = file.replace('.csv', '')
            file_path = os.path.join(BRONZE_DIR, file)
            
            try:
                # Register in the registry
                register_table(table_name, file_path)
                registry[table_name] = file_path
                registered_count += 1
                logger.debug(f"Registered table: {table_name}")
            except Exception as e:
                logger.error(f"Failed to register {table_name}: {e}")
                continue
    
    logger.info(f"✅ Registered {registered_count} table(s): {list(registry.keys())}")
    return registry


# -------------------------------------------------
# Batch Runner
# -------------------------------------------------
def run_batch(max_files: int = None):
    """
    Run batch processing of tables.
    
    Args:
        max_files: Maximum number of files to process (None = all)
    """
    # Step 1: Register bronze tables
    logger.info("=" * 60)
    logger.info("STEP 1: Registering bronze tables")
    logger.info("=" * 60)
    register_bronze_tables()
    
    # Step 2: Load workflow
    logger.info("=" * 60)
    logger.info("STEP 2: Building workflow")
    logger.info("=" * 60)
    workflow = build_workflow()
    registry = load_registry()
    
    if not registry:
        logger.error("No tables found in registry. Exiting.")
        return

    logger.info("Starting batch processing of CSV files from registry")

    all_pending_reviews = _load_reviews()

    tables = list(registry.items())
    if max_files and max_files > 0:
        tables = list(islice(tables, max_files))
        logger.info(f"Processing limited to {len(tables)} table(s)")

    # Step 3: Process tables
    logger.info("=" * 60)
    logger.info(f"STEP 3: Processing {len(tables)} table(s)")
    logger.info("=" * 60)
    
    processed_count = 0
    skipped_count = 0
    failed_count = 0

    for table_name, file_path in tables:
        path = os.path.join(BRONZE_DIR, f"{table_name}.csv")

        if not os.path.exists(path):
            logger.warning(f"File missing for {table_name}, skipping")
            skipped_count += 1
            continue

        # Skip tables with pending reviews
        if any(
            sess.get("status") == "pending" and sess.get("table") == table_name
            for sess in all_pending_reviews.values()
        ):
            logger.warning(f"Skipping {table_name}: HITL review still pending")
            skipped_count += 1
            continue

        try:
            logger.info(f"--- Running workflow for table: {table_name} ---")

            df = pd.read_csv(path, low_memory=False)
            config = {
                "configurable": {"thread_id": table_name},
                "recursion_limit": 500  # Increase limit to allow HITL polling
            }

            result = workflow.invoke(
                {"table_name": table_name, "df": df},
                config
            )

            # If HITL is pending, stop safely
            if result.get("hitl_status") == "pending":
                logger.info(
                    f"HITL approval pending for {table_name}. "
                    "Workflow paused cleanly."
                )
                skipped_count += 1
                continue

            logger.info(
                f"✅ Completed {table_name}. "
                f"Metrics: {result.get('metrics')}"
            )
            processed_count += 1

        except Exception as e:
            logger.error(
                f"❌ Workflow failed for {table_name}",
                exc_info=True
            )
            failed_count += 1
            continue

    # Summary
    logger.info("=" * 60)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"✅ Processed: {processed_count} table(s)")
    logger.info(f"⏸️  Pending HITL: {skipped_count} table(s)")
    logger.info(f"❌ Failed: {failed_count} table(s)")
    logger.info("=" * 60)


# -------------------------------------------------
# Entrypoint
# -------------------------------------------------
if __name__ == "__main__":
    run_batch(max_files=1)

