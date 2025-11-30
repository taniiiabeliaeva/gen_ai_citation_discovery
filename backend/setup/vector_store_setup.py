# setup/vector_store_setup.py
import pandas as pd
import os
from tqdm import tqdm
from vectordb_utils.document_manager import DocumentManager

# --- CONFIGURATION ---
CSV_FILE_PATH = "data/works_final.csv"
VECTOR_DB_DIR = "data/faiss_db"


def setup_environment_and_create_vector_store():
    """Creates FAISS vector store from CSV file of papers and PDFs."""

    document_manager = DocumentManager()

    df = pd.read_csv(CSV_FILE_PATH)

    for index, row in tqdm(df.iterrows(), total=len(df)):
        pdf_path = row["pdf_paths"]
        title = row["title"]
        if not os.path.exists(pdf_path):
            continue

        try:
            document_manager.process_and_index_pdf(pdf_path, title)
        except Exception as e:
            print(f"Failed to process PDF {pdf_path}. Error: {e}")

    print("Vector store created successfully.")


if __name__ == "__main__":
    setup_environment_and_create_vector_store()
