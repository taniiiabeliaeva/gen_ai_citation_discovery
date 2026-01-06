import os
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from argparse import ArgumentParser
from vectordb_utils.document_manager import DocumentManager


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--test", action="store_true", default=False)
    return parser.parse_args()


# --- CONFIGURATION ---
CSV_FILE_PATH = "data/works_final.csv"
VECTOR_DB_DIR = "data/faiss_db"

load_dotenv()


def setup_environment_and_create_vector_store(test=False):
    """Creates FAISS vector store from CSV file of papers and PDFs."""

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
    setup_environment_and_create_vector_store(test=args.test)
