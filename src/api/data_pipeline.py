"""
Drug Extraction Pipeline Orchestrator

Pipeline stages:

1. Extract tables from Ethiopia Essential Medicines List (PDF)
2. Clean extracted data
3. Normalize drug information

Outputs structured dataset for downstream systems
(search, embeddings, or analytics)
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from src.ingestion.loader import extract_tables
from src.processing.cleaner import clean_dataframe
from src.processing.drug_normalizer import normalize_drugs
from src.utils.logger import get_logger

logger = get_logger("drug_pipeline")

# -------------------------------------------------------------------
# Default Paths
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_PDF = PROJECT_ROOT / "data/raw/ethiopia-2020.pdf"

EXTRACTED_FILE = PROJECT_ROOT / "data/intermediate/extracted_drugs.csv"
CLEAN_FILE = PROJECT_ROOT / "data/intermediate/cleaned_drugs.csv"
FINAL_FILE = PROJECT_ROOT / "data/processed/final_drugs.csv"


# -------------------------------------------------------------------
# Pipeline Steps
# -------------------------------------------------------------------


def run_extraction():
    """Step 1: Extract tables from PDF"""

    logger.info("=== Extraction Stage Started ===")

    df = extract_tables(RAW_PDF)

    if df.empty:
        logger.warning("No rows extracted.")
        return

    EXTRACTED_FILE.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(EXTRACTED_FILE, index=False)

    logger.info("Extraction completed: %d rows", len(df))
    logger.info("Saved to %s", EXTRACTED_FILE)


def run_cleaning():
    """Step 2: Clean extracted data"""

    logger.info("=== Cleaning Stage Started ===")

    if not EXTRACTED_FILE.exists():
        logger.error("Extracted file not found: %s", EXTRACTED_FILE)
        return

    df = pd.read_csv(EXTRACTED_FILE)

    df = clean_dataframe(df)

    CLEAN_FILE.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(CLEAN_FILE, index=False)

    logger.info("Cleaning completed: %d rows", len(df))
    logger.info("Saved to %s", CLEAN_FILE)


def run_normalization():
    """Step 3: Normalize drug metadata"""

    logger.info("=== Normalization Stage Started ===")

    if not CLEAN_FILE.exists():
        logger.error("Cleaned file not found: %s", CLEAN_FILE)
        return

    df = pd.read_csv(CLEAN_FILE)

    df = normalize_drugs(df)

    FINAL_FILE.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(FINAL_FILE, index=False)

    logger.info("Normalization completed: %d rows", len(df))
    logger.info("Saved to %s", FINAL_FILE)


def run_pipeline():
    """Run full pipeline"""

    logger.info("=== Starting Full Drug Pipeline ===")

    try:
        run_extraction()
        run_cleaning()
        run_normalization()

        logger.info("=== Pipeline Completed Successfully ===")

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Drug Extraction Pipeline for Ethiopia Essential Medicines"
    )

    parser.add_argument(
        "--extract",
        action="store_true",
        help="Run extraction stage",
    )

    parser.add_argument(
        "--clean",
        action="store_true",
        help="Run cleaning stage",
    )

    parser.add_argument(
        "--normalize",
        action="store_true",
        help="Run normalization stage",
    )

    parser.add_argument(
        "--run_pipeline",
        action="store_true",
        help="Run full pipeline",
    )

    return parser.parse_args()


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

if __name__ == "__main__":

    args = parse_args()

    if not (args.extract or args.clean or args.normalize or args.run_pipeline):
        logger.error("No action specified.")
        sys.exit(1)

    if args.extract:
        run_extraction()

    if args.clean:
        run_cleaning()

    if args.normalize:
        run_normalization()

    if args.run_pipeline:
        run_pipeline()
