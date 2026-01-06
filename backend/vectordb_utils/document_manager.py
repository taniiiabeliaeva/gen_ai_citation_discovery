# api/document_manager.py
import os
import faiss
import uuid
import pandas as pd
from typing import List, Dict
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_community.docstore.in_memory import InMemoryDocstore


class DocumentManager:

    DATA_DIR = "data"
    VECTOR_DB_DIR = "data/faiss_db"

    def __init__(self, google_api_key: str = None):

        self.embeddings = GoogleGenerativeAIEmbeddings(
            model="models/gemini-embedding-001", google_api_key=google_api_key
        )
        self.vector_store = FAISS(
            embedding_function=self.embeddings,
            index=faiss.IndexFlatL2(3072),
            docstore=InMemoryDocstore(),
            index_to_docstore_id={},
        )

        self.load_index()

    def process_and_index_pdf(self, file_path: str = None, metadata: Dict = None):
        """
        Process a PDF file and add it to the vector store.
        - If file_path is not provided, we embed title and abstract.
        - If file_path is provided, we embed the PDF.
        - If metadata is provided, we add it to the Vector DB document metadata.


        Args:
            file_path: Path to the PDF file
            metadata: Metadata for the document
        """

        if not file_path and not metadata:
            raise ValueError("Must provide either file_path or metadata.")

        if file_path:
            pdfs_paths = [path.strip() for path in file_path.split(";")]
            for pdf_path in pdfs_paths:
                # Read PDF
                try:
                    loader = PyMuPDFLoader(os.path.join(self.DATA_DIR, pdf_path))
                    docs = loader.load()
                except Exception as e:
                    raise Exception(f"Failed to load PDF: {str(e)}")

                for doc in docs:
                    doc.metadata.update(metadata)

                # Split into chunks
                text_splitter = RecursiveCharacterTextSplitter(
                    chunk_size=1000, chunk_overlap=200
                )
                chunks = text_splitter.split_documents(docs)

                # Add to vector store
                self.vector_store.add_documents(chunks)

        else:
            doc = Document(
                page_content=metadata.get("title", "")
                + " "
                + metadata.get("abstract_en", ""),
                metadata=metadata,
            )
            self.vector_store.add_documents([doc])

    def list_documents(self) -> List[Dict]:
        """
        Retrieves all documents that have a PDF file path and their metadata from the InMemoryDocstore
        associated with the FAISS vector store.

        Returns:
            A list of dictionaries, where each dictionary represents a document
            and includes 'title', 'file_path' and 'metadata'.
        """
        # 1. Access the docstore, which holds the actual Document objects
        docstore = self.vector_store.docstore

        # 2. Extract the list of Document objects from the docstore
        # The ._dict attribute of InMemoryDocstore is the underlying dictionary
        all_docs: List[Document] = list(docstore._dict.values())
        all_pdf_docs = [
            doc for doc in all_docs if doc.metadata.get("file_path", "") != ""
        ]

        # 3. Format the documents into a list of dictionaries
        results = dict()
        for doc in all_pdf_docs:
            if doc.metadata.get("file_path") not in results:
                results[doc.metadata.get("file_path")] = {
                    "title": doc.metadata.get("title", ""),
                    "file_path": doc.metadata.get("file_path", ""),
                    "total_pages": doc.metadata.get("total_pages", ""),
                    "cited_by_count": doc.metadata.get("cited_by_count", ""),
                }

        return list(results.values())

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

    def save_index(self, index_path: str = None):
        """
        Save the FAISS vector store to disk.

        Args:
            index_path: Path where to save the FAISS index.
                       If None, uses the default VECTOR_DB_DIR.
        """
        if index_path is None:
            index_path = self.VECTOR_DB_DIR

        try:
            # Create directory if it doesn't exist
            os.makedirs(index_path, exist_ok=True)

            # Save the FAISS index
            self.vector_store.save_local(index_path)
            print(f"FAISS index saved successfully to {index_path}")
            return True
        except Exception as e:
            print(f"Error saving FAISS index: {e}")
            return False

    def load_index(self, index_path: str = None):
        """
        Load a FAISS vector store from disk.

        Args:
            index_path: Path from where to load the FAISS index.
                       If None, uses the default VECTOR_DB_DIR.

        Returns:
            True if successful, False otherwise
        """
        if index_path is None:
            index_path = self.VECTOR_DB_DIR

        try:
            # Check if the index exists
            if not os.path.exists(index_path):
                print(f"FAISS index not found at {index_path}")
                return False

            # Load the FAISS index
            self.vector_store = FAISS.load_local(
                index_path, self.embeddings, allow_dangerous_deserialization=True
            )
            print(f"FAISS index loaded successfully from {index_path}")
            return True
        except Exception as e:
            print(f"Error loading FAISS index: {e}")
            return False
