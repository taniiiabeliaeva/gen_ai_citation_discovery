import os
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from argparse import ArgumentParser
import shutil
from vectordb_utils.document_manager import DocumentManager
from llm.model import EmbeddingModel

def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--test", action="store_true", default=False)
    parser.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="Delete the existing vector store directory before indexing.",
    )
    parser.add_argument(
        "--embedding_model",
        type=str,
        default="GEMINI_EMBEDDING_001",
        help="Embedding model enum name from EmbeddingModel, GEMINI_EMBEDDING_001 / MISTRAL_EMBEDDING_5",
    )

    return parser.parse_args()


# --- CONFIGURATION ---
CSV_FILE_PATH = "data/works_final.csv"

load_dotenv()


def setup_environment_and_create_vector_store(test=False, reset=False, embedding_model="GEMINI_EMBEDDING_001"):
    """Creates FAISS vector store from CSV file of papers and PDFs."""

    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(
            f"CSV file not found at {CSV_FILE_PATH}. Please run setup scripts first."
        )

    try:
        embedding_model = EmbeddingModel[embedding_model]
    except KeyError:
        valid = ", ".join([m.name for m in EmbeddingModel])
        raise ValueError(f"Unknown embedding model '{embedding_model}'. Valid: {valid}")

    if reset:
        if embedding_model == EmbeddingModel.GEMINI_EMBEDDING_001:
            if os.path.exists("data/faiss_db_gemini"):
                shutil.rmtree("data/faiss_db_gemini")
                print("Existing vector store directory deleted.")
        elif embedding_model == EmbeddingModel.MISTRAL_EMBEDDING_5:
            if os.path.exists("data/faiss_db_mistral"):
                shutil.rmtree("data/faiss_db_mistral")
                print("Existing vector store directory deleted.")
        else:
            raise ValueError(f"Unsupported embedding model: {embedding_model}")
        
    document_manager = DocumentManager(embedding_model)
    df = pd.read_csv(CSV_FILE_PATH)

    if test:
        df = df.head(20)

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

    setup_environment_and_create_vector_store(test=args.test, reset=args.reset, embedding_model=args.embedding_model)
