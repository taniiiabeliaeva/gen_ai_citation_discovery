# tools/rag_tools.py
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from tools.data_utils import _get_openalex_id_from_title
from core.graph_state import ResearchIdeas  # Import Pydantic models TODO
from vectordb_utils.document_manager import DocumentManager
from dotenv import load_dotenv
import os

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

_DOCUMENT_MANAGER: DocumentManager = None


def set_document_manager(document_manager: DocumentManager) -> None:
    global _DOCUMENT_MANAGER
    _DOCUMENT_MANAGER = document_manager


def _get_vector_store():
    global _DOCUMENT_MANAGER
    if _DOCUMENT_MANAGER is None:
        _DOCUMENT_MANAGER = DocumentManager(GOOGLE_API_KEY)
    return _DOCUMENT_MANAGER.vector_store


def _canonicalize_file_path(file_path: str) -> str:
    global _DOCUMENT_MANAGER
    if _DOCUMENT_MANAGER is None:
        _DOCUMENT_MANAGER = DocumentManager(GOOGLE_API_KEY)
    return _DOCUMENT_MANAGER._canonicalize_file_path(file_path)

# --- RAG Tool List ---
ALL_RAG_TOOLS = []


@tool
def query_paper_for_answer(user_query: str, paper_file_path: str) -> str:
    """
    Analyzes the content of a specific local paper to answer a question or provide
    a detailed summary using RAG. Returns the answer and explicitly cites the source PDF.
    """
    canonical_path = _canonicalize_file_path(paper_file_path)
    retriever = _get_vector_store().as_retriever(
        search_kwargs={"k": 3, "filter": {"file_path": canonical_path}}
    )
    relevant_docs = retriever.invoke(user_query)

    if not relevant_docs:
        return f"Could not find relevant text chunks for the paper: {paper_file_path} (canonical: {canonical_path})."

    # 1. Collect unique source metadata from retrieved chunks
    source_metadata = set()
    for doc in relevant_docs:
        source_metadata.add((doc.metadata.get("title"), doc.metadata.get("source")))

    source_list = "\n".join(
        [f"- Title: {t}, Source Path: {s}" for t, s in source_metadata]
    )
    context = "\n---\n".join([doc.page_content for doc in relevant_docs])

    # 2. Use LLM to synthesize the answer and cite the source
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1)

    rag_prompt = f"""
    You are a research analyst. Use the CONTEXT provided below to answer the USER QUESTION.
    SYNTHESIZED ANSWER:
    """
    answer = llm.invoke(
        rag_prompt + context + f"\n\nUSER QUESTION: {user_query}"
    ).content

    # Append the sources explicitly to the final string output
    return f"{answer}\n\n--- SOURCES USED ---\n{source_list}"


@tool
def generate_research_ideas(paper_file_path: str, num_ideas: int = 3) -> str:
    """
    Analyzes the paper's content using RAG to identify limitations or future work
    sections and generates novel research questions based on them.

    Args:
        paper_file_path: Path to the paper file that the user mentions in the query.
        num_ideas: The number of research ideas to generate (default is 3).

    Returns:
        A list of generated research ideas.
    """
    # 1. Retrieve sections relevant to limitations and future work
    canonical_path = _canonicalize_file_path(paper_file_path)
    retriever = _get_vector_store().as_retriever(
        search_kwargs={"k": 5, "filter": {"file_path": canonical_path}}
    )
    # Search specifically for limitations/future work
    retrieval_query = f"What are the limitations, gaps, and suggested future work for the paper {canonical_path}?"
    relevant_docs = retriever.invoke(retrieval_query)

    if not relevant_docs:
        return f"Could not retrieve enough context on limitations/future work for {canonical_path} to generate ideas."

    context = "\n---\n".join([doc.page_content for doc in relevant_docs])

    # 2. Use a specialized LLM prompt for generation
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.3)

    generator_prompt = f"""
    You are an expert academic reviewer. Your task is to critically analyze the provided CONTEXT, which contains information about the limitations and future work of a scholarly paper.
    
    Based ONLY on the context, generate {num_ideas} distinct, novel, and specific research questions or projects that address the identified limitations or extend the future work suggestions.
    
    Format your output clearly, using a numbered list, with each item containing the new research question and a brief justification/link back to the context.
    
    CONTEXT:
    ---
    {context}
    ---
    
    Generated Research Ideas:
    """
    response = llm.invoke(generator_prompt)
    return response.content


@tool
def answer_general_question(user_query: str) -> str:
    """
    Answers general questions by searching across ALL indexed documents in the vector database.
    This is the PRIMARY tool for answering user questions about document content when you don't
    know which specific paper to search.

    Use this tool when:
    - The user asks a general question without specifying a paper
    - You want to find relevant information across all available documents
    - The user asks "What do you know about X?" or "Explain Y"

    Args:
        user_query: The user's question or query (e.g., "What is Spectre?", "Explain hardware vulnerabilities")

    Returns:
        A comprehensive answer with citations from the most relevant documents.
    """
    print(f"\n[RAG TOOL] Searching all documents for: {user_query}")

    # Retrieve the top 5 most relevant chunks across ALL documents (no filter)
    retriever = _get_vector_store().as_retriever(
        search_kwargs={"k": 5}  # No filter = search all documents
    )
    relevant_docs = retriever.invoke(user_query)

    if not relevant_docs:
        return "I couldn't find any relevant information in the indexed documents. Please make sure documents are uploaded and indexed."

    print(f"[RAG TOOL] Found {len(relevant_docs)} relevant chunks")

    # 1. Collect unique source metadata from retrieved chunks
    source_metadata = set()
    for doc in relevant_docs:
        source_metadata.add((doc.metadata.get("title"), doc.metadata.get("source")))

    source_list = "\n".join(
        [f"- Title: {t}, Source Path: {s}" for t, s in source_metadata]
    )
    context = "\n---\n".join([doc.page_content for doc in relevant_docs])

    # 2. Use LLM to synthesize the answer and cite the sources
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1)

    rag_prompt = f"""
    You are a research assistant. Use the CONTEXT provided below to answer the USER QUESTION.
    
    IMPORTANT INSTRUCTIONS:
    - Base your answer ONLY on the information provided in the CONTEXT
    - If the context doesn't contain enough information to fully answer the question, say so
    - Cite specific information from the context when possible
    - Be comprehensive but concise
    - If multiple documents are relevant, synthesize information from all of them
    
    CONTEXT:
    ---
    {context}
    ---
    
    USER QUESTION: {user_query}
    
    SYNTHESIZED ANSWER:
    """

    answer = llm.invoke(rag_prompt).content

    # Append the sources explicitly to the final string output
    return f"{answer}\n\n--- SOURCES USED ---\n{source_list}"


@tool
def recommend_relevant_papers(topic: str, num_papers: int = 10) -> str:
    """
    Recommends papers relevant to a specific research topic using semantic search.
    Returns a ranked list of papers with relevance scores.

    Use this tool when:
    - User asks "Which papers are relevant for X?"
    - User wants to discover papers on a specific topic
    - User needs paper recommendations for their research

    Args:
        topic: The research topic or query (e.g., "hardware security vulnerabilities", "Spectre attacks")
        num_papers: Number of papers to recommend (default: 10)

    Returns:
        JSON string with ranked papers and relevance scores
    """
    print(f"\n[RECOMMEND TOOL] Searching for papers on topic: {topic}")
    print(f"[RECOMMEND TOOL] Requesting top {num_papers} papers")

    # Use similarity search to find relevant documents
    retriever = _get_vector_store().as_retriever(search_kwargs={"k": num_papers})

    # Perform search
    relevant_docs = retriever.invoke(topic)

    if not relevant_docs:
        return "No papers found matching the topic. Please try a different query or upload more documents."

    print(f"[RECOMMEND TOOL] Found {len(relevant_docs)} relevant documents")

    # Extract unique papers with scores
    papers_dict = {}
    for doc in relevant_docs:
        file_path = doc.metadata.get("source") or doc.metadata.get("file_path")
        title = doc.metadata.get("title", "Untitled")

        # Calculate relevance score (normalized similarity)
        # Note: FAISS returns documents in order of relevance
        # We'll assign scores based on position (first = highest)
        if file_path not in papers_dict:
            position = len(papers_dict)
            # Score from 1.0 (first) to 0.5 (last)
            score = 1.0 - (position / (num_papers * 2))
            papers_dict[file_path] = {
                "title": title,
                "file_path": file_path,
                "relevance_score": round(score, 2),
            }

    # Convert to list and sort by score
    papers_list = list(papers_dict.values())
    papers_list.sort(key=lambda x: x["relevance_score"], reverse=True)

    # Format response
    import json

    result = {"topic": topic, "num_results": len(papers_list), "papers": papers_list}

    print(f"[RECOMMEND TOOL] Returning {len(papers_list)} unique papers")
    return json.dumps(result, indent=2)


ALL_RAG_TOOLS.append(query_paper_for_answer)
ALL_RAG_TOOLS.append(generate_research_ideas)
ALL_RAG_TOOLS.append(answer_general_question)
ALL_RAG_TOOLS.append(recommend_relevant_papers)
