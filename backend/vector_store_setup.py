import os
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from argparse import ArgumentParser
import shutil
from vectordb_utils.document_manager import DocumentManager


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--test", action="store_true", default=False)
    parser.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="Delete the existing vector store directory before indexing.",
    )
    return parser.parse_args()


# --- CONFIGURATION ---
CSV_FILE_PATH = "data/works_final.csv"
VECTOR_DB_DIR = "data/faiss_db"

load_dotenv()


def setup_environment_and_create_vector_store(test=False):
    """Creates FAISS vector store from CSV file of papers and PDFs."""

    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(
            f"CSV file not found at {CSV_FILE_PATH}. Please run setup scripts first."
        )

    document_manager = DocumentManager(google_api_key=os.getenv("GOOGLE_API_KEY"))
    df = pd.read_csv(CSV_FILE_PATH)

    if test:
        df = df.head(16)

    for index, row in tqdm(df.iterrows(), total=len(df)):
        pdf_path = row["pdf_paths"] if not pd.isnull(row["pdf_paths"]) else None
        metadata = df.loc[index].dropna().to_dict()

        try:
            document_manager.process_and_index_pdf(pdf_path, metadata)
        except Exception as e:
            print(f"Failed to process PDF {pdf_path}. Error: {e}")

    print("Vector store created successfully.")

    document_manager.save_index()
    print("Index saved successfully.")


if __name__ == "__main__":
    args = parse_args()
    if args.reset and os.path.exists(VECTOR_DB_DIR):
        shutil.rmtree(VECTOR_DB_DIR)

    setup_environment_and_create_vector_store(test=args.test)
