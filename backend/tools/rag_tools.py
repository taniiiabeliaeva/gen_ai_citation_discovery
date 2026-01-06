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
VECTOR_STORE = DocumentManager(GOOGLE_API_KEY).vector_store

# --- RAG Tool List ---
ALL_RAG_TOOLS = []


@tool
def query_paper_for_answer(user_query: str, paper_file_path: str) -> str:
    """
    Analyzes the content of a specific local paper to answer a question or provide
    a detailed summary using RAG. Returns the answer and explicitly cites the source PDF.
    """
    retriever = VECTOR_STORE.as_retriever(
        search_kwargs={"k": 3, "filter": {"file_path": paper_file_path}}
    )
    relevant_docs = retriever.invoke(user_query)

    if not relevant_docs:
        return f"Could not find relevant text chunks for the paper: {paper_file_path}."

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
    retriever = VECTOR_STORE.as_retriever(
        search_kwargs={"k": 5, "filter": {"file_path": paper_file_path}}
    )
    # Search specifically for limitations/future work
    retrieval_query = f"What are the limitations, gaps, and suggested future work for the paper {paper_title_keywords}?"
    relevant_docs = retriever.invoke(retrieval_query)

    if not relevant_docs:
        return f"Could not retrieve enough context on limitations/future work for {paper_title_keywords} to generate ideas."

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
    retriever = VECTOR_STORE.as_retriever(
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


ALL_RAG_TOOLS.append(query_paper_for_answer)
ALL_RAG_TOOLS.append(generate_research_ideas)
ALL_RAG_TOOLS.append(answer_general_question)
