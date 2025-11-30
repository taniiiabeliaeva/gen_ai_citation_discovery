# api/document_manager.py
import os
import uuid
from typing import List, Dict
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import FAISS
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
UPLOAD_DIR = "data/pdfs"

# Ensure upload directory exists
os.makedirs(UPLOAD_DIR, exist_ok=True)


class DocumentManager:

    VECTOR_DB_DIR = "data/faiss_db"

    def __init__(self):
        self.embeddings = GoogleGenerativeAIEmbeddings(
            model="models/embedding-001", api_key=GEMINI_API_KEY
        )
        self.vector_store = FAISS(
            persist_directory=self.VECTOR_DB_DIR, embedding_function=self.embeddings
        )

    def process_and_index_pdf(self, file_path: str, filename: str) -> Dict:
        """
        Process a PDF file and add it to the vector store.

        Args:
            file_path: Path to the PDF file
            filename: Original filename
            doc_id: Unique document ID

        Returns:
            Dictionary with document metadata
        """
        try:
            # Read PDF
            loader = PyMuPDFLoader(file_path)

            docs = loader.load()

            # Split into chunks
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=1000, chunk_overlap=200
            )
            chunks = text_splitter.split_documents(docs)

            # Add to vector store
            self.vector_store.add_documents(chunks)

            return {
                "doc_id": self._generate_doc_id(),
                "filename": filename,
                "page_count": len(docs),
                "chunk_count": len(chunks),
                "status": "indexed",
            }
        except Exception as e:
            raise Exception(f"Failed to process PDF: {str(e)}")

    def list_documents(self) -> List[Dict]:
        """
        List all documents in the vector store.

        Returns:
            List of document metadata dictionaries
        """
        try:
            # Get all documents from the collection
            collection = self.vector_store._collection

            # Get unique documents by doc_id
            all_metadata = collection.get()

            if not all_metadata or "metadatas" not in all_metadata:
                return []

            # Extract unique documents
            docs_dict = {}
            for metadata in all_metadata["metadatas"]:
                doc_id = metadata.get("doc_id")
                if doc_id and doc_id not in docs_dict:
                    docs_dict[doc_id] = {
                        "doc_id": doc_id,
                        "filename": metadata.get("title", "Unknown"),
                        "source": metadata.get("source", ""),
                        "page_count": metadata.get("page_count", 0),
                    }

            return list(docs_dict.values())
        except Exception as e:
            print(f"Error listing documents: {e}")
            return []

    def delete_document(self, doc_id: str) -> bool:
        """
        Delete a document from the vector store.

        Args:
            doc_id: Document ID to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            collection = self.vector_store._collection

            # Get all IDs with matching doc_id
            results = collection.get(where={"doc_id": doc_id})

            if results and "ids" in results and results["ids"]:
                collection.delete(ids=results["ids"])

                # Also delete the file if it exists
                docs = self.list_documents()
                for doc in docs:
                    if doc["doc_id"] == doc_id and os.path.exists(doc["source"]):
                        os.remove(doc["source"])
                        break

                return True
            return False
        except Exception as e:
            print(f"Error deleting document: {e}")
            return False

    def _generate_doc_id(self) -> str:
        """Generate a unique document ID."""
        return str(uuid.uuid4())
