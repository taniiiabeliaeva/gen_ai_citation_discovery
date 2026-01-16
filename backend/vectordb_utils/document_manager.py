# api/document_manager.py
import os
import faiss
import uuid
import pandas as pd
from typing import List, Dict, Optional
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_community.docstore.in_memory import InMemoryDocstore
from llm.model import EmbeddingModel, get_model_instance


class DocumentManager:

    DATA_DIR = "data"
    VECTOR_DB_DIR = "data/faiss_db"

    def __init__(self, model: EmbeddingModel):
        self.model = model
        self.embeddings = get_model_instance(model)
        
        # Get the appropriate vector size and directory for this model
        if model == EmbeddingModel.GEMINI_EMBEDDING_001:
            vector_size = 3072
            self.VECTOR_DB_DIR = "data/faiss_db_gemini"
        elif model == EmbeddingModel.MISTRAL_EMBEDDING_5:
            vector_size = 4096
            self.VECTOR_DB_DIR = "data/faiss_db_mistral"
        else:
            raise ValueError(f"Unsupported embedding model: {model}")

        self.vector_store = FAISS(
            embedding_function=self.embeddings,
            index=faiss.IndexFlatL2(vector_size),
            docstore=InMemoryDocstore(),
            index_to_docstore_id={},
        )

        self._indexed_file_paths = set()
        self.load_index()
        self._rebuild_indexed_file_paths_cache()

    def _canonicalize_file_path(self, file_path: Optional[str]) -> str:
        if not file_path:
            return ""

        candidate = str(file_path).strip().replace("\\", "/")

        if candidate.startswith("./data/"):
            candidate = candidate[len("./data/") :]
        elif candidate.startswith("data/"):
            candidate = candidate[len("data/") :]

        if os.path.isabs(candidate):
            resolved_abs = os.path.abspath(candidate)
            data_dir_abs = os.path.abspath(self.DATA_DIR)
            try:
                if os.path.commonpath([resolved_abs, data_dir_abs]) == data_dir_abs:
                    candidate = os.path.relpath(resolved_abs, data_dir_abs)
            except ValueError:
                pass

        return candidate.replace("\\", "/")

    def _rebuild_indexed_file_paths_cache(self) -> None:
        docstore = self.vector_store.docstore
        all_docs: List[Document] = list(docstore._dict.values())
        indexed = set()
        for doc in all_docs:
            fp = self._canonicalize_file_path(doc.metadata.get("file_path", ""))
            if fp:
                indexed.add(fp)
        self._indexed_file_paths = indexed

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

        metadata = metadata or {}

        if file_path:
            indexed_files: List[str] = []
            skipped_files: List[str] = []
            pdfs_paths = [path.strip() for path in file_path.split(";") if path.strip()]
            for pdf_path in pdfs_paths:
                normalized_input_path = os.path.normpath(pdf_path)

                candidate_paths: List[str] = [normalized_input_path]
                if not os.path.isabs(normalized_input_path):
                    candidate_paths.append(
                        os.path.normpath(os.path.join(self.DATA_DIR, normalized_input_path))
                    )

                resolved_path: Optional[str] = next(
                    (p for p in candidate_paths if os.path.exists(p)), None
                )
                if not resolved_path:
                    raise FileNotFoundError(
                        f"Failed to load PDF: File path {candidate_paths[0]} is not a valid file or url"
                    )

                # Read PDF
                loader = PyMuPDFLoader(resolved_path)
                docs = loader.load()

                resolved_abs = os.path.abspath(resolved_path)
                data_dir_abs = os.path.abspath(self.DATA_DIR)

                effective_file_path: str
                try:
                    if os.path.commonpath([resolved_abs, data_dir_abs]) == data_dir_abs:
                        effective_file_path = os.path.relpath(resolved_abs, data_dir_abs)
                    else:
                        effective_file_path = (
                            resolved_abs
                            if os.path.isabs(normalized_input_path)
                            else normalized_input_path
                        )
                except ValueError:
                    print("ValueError in commonpath check")
                    effective_file_path = (
                        resolved_abs
                        if os.path.isabs(normalized_input_path)
                        else normalized_input_path
                    )

                effective_file_path = self._canonicalize_file_path(effective_file_path)

                if effective_file_path in self._indexed_file_paths:
                    skipped_files.append(effective_file_path)
                    continue

                base_metadata = dict(metadata)
                base_metadata.setdefault(
                    "title", os.path.splitext(os.path.basename(effective_file_path))[0]
                )
                base_metadata["file_path"] = effective_file_path
                base_metadata.setdefault("total_pages", len(docs))

                for doc in docs:
                    doc.metadata.update(base_metadata)

                # Split into chunks
                text_splitter = RecursiveCharacterTextSplitter(
                    chunk_size=1000, chunk_overlap=200
                )
                chunks = text_splitter.split_documents(docs)

                # Add to vector store
                try:
                    self.vector_store.add_documents(chunks)
                except Exception as e:
                    raise RuntimeError(f"Failed to add documents to vector store: {e}")

                indexed_files.append(effective_file_path)
                self._indexed_file_paths.add(effective_file_path)

            return {
                "indexed": indexed_files,
                "skipped": skipped_files,
                "count": len(indexed_files),
            }

        else:
            print("[DocumentManager] Indexing document with title and abstract only.")
            doc = Document(
                page_content=metadata.get("title", "")
                + " "
                + metadata.get("abstract_en", ""),
                metadata=metadata,
            )
            self.vector_store.add_documents([doc])
            return {"indexed": [], "count": 0}

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
        all_pdf_docs = [doc for doc in all_docs if doc.metadata.get("file_path", "") != ""]

        # 3. Format the documents into a list of dictionaries
        results = dict()
        for doc in all_pdf_docs:
            file_path = self._canonicalize_file_path(doc.metadata.get("file_path", ""))
            if not file_path:
                continue

            if file_path not in results:
                results[file_path] = {
                    "title": doc.metadata.get("title", ""),
                    "file_path": file_path,
                    "total_pages": doc.metadata.get("total_pages", ""),
                    "cited_by_count": doc.metadata.get("cited_by_count", ""),
                }

        return list(results.values())

    def delete_document(self, file_path: str) -> bool:
        """
        Delete all chunks belonging to a given PDF (by file_path) from the FAISS store,
        then rebuild the index.
        """
        try:
            target = self._canonicalize_file_path(file_path)
            
            # Also try with pdfs prefix for matching
            target_with_prefix = self._canonicalize_file_path(f"pdfs/{file_path}")
            
            if not target:
                return False

            # 1) Keep only docs NOT matching target
            docstore = self.vector_store.docstore
            old_ids = list(docstore._dict.keys())

            kept_texts = []
            kept_metas = []
            removed_any = False

            for _id in old_ids:
                doc = docstore._dict[_id]
                fp = self._canonicalize_file_path(doc.metadata.get("file_path", ""))
                if fp == target or fp == target_with_prefix:
                    removed_any = True
                else:
                    kept_texts.append(doc.page_content)
                    kept_metas.append(doc.metadata)

            if not removed_any:
                return False

            # 2) Rebuild FAISS from kept docs
            # (this regenerates embeddings; simplest + consistent)
            new_store = FAISS.from_texts(
                texts=kept_texts,
                embedding=self.embeddings,
                metadatas=kept_metas,
            )

            self.vector_store = new_store
            self._rebuild_indexed_file_paths_cache()
            return True
        except Exception as e:
            print(f"Error deleting document: {e}")
            import traceback
            traceback.print_exc()
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
            self._rebuild_indexed_file_paths_cache()
            return True
        except Exception as e:
            print(f"Error loading FAISS index: {e}")
            return False
