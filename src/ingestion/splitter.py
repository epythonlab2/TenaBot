from pathlib import Path

import camelot
import pandas as pd

# -------------------------------
# Paths (relative to project root)
# -------------------------------
PROJECT_ROOT = (
    Path(__file__).resolve().parent.parent.parent
)  # src/ingestion/loader -> project root
RAW_PDF_PATH = PROJECT_ROOT / "data/raw/ethiopia-2020.pdf"
PROCESSED_CSV_PATH = PROJECT_ROOT / "data/processed/processed_drugs.csv"


# -------------------------------
# Functions
# -------------------------------
def extract_drug_tables(pdf_path: Path) -> pd.DataFrame:
    """Extract tables containing drug info from a PDF."""
    if not pdf_path.is_file():
        raise FileNotFoundError(f"PDF file not found: {pdf_path.resolve()}")

    tables = camelot.read_pdf(str(pdf_path), pages="all", flavor="stream")
    drug_tables = []

    for table in tables:
        df = table.df.copy()
        headers = [str(h).strip().lower() for h in df.iloc[0].tolist()]
        if any(header in headers for header in ["generic name", "dosage", "strength"]):
            drug_tables.append(df.iloc[1:].reset_index(drop=True))

    if not drug_tables:
        raise ValueError("No drug tables found in the PDF.")

    return pd.concat(drug_tables, ignore_index=True)


def save_to_csv(df: pd.DataFrame, path: Path):
    """Save DataFrame to CSV, creating directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


# -------------------------------
# Main
# -------------------------------
def main():
    try:
        all_drugs = extract_drug_tables(RAW_PDF_PATH)
        save_to_csv(all_drugs, PROCESSED_CSV_PATH)
        print(f"Processed data saved to: {PROCESSED_CSV_PATH.resolve()}")
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    main()
