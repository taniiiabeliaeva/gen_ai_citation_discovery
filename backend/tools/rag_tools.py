# tools/rag_tools.py
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import Chroma
import os
from dotenv import load_dotenv

from tools.data_utils import _get_openalex_id_from_title
from core.graph_state import ResearchIdeas # Import Pydantic models

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
VECTOR_DB_DIR = "data/chroma_db"

# Global RAG objects
# Note: The VECTOR_STORE must be initialized by vector_store_setup.py first.
EMBEDDINGS = GoogleGenerativeAIEmbeddings(model="models/embedding-001", api_key=GEMINI_API_KEY)
VECTOR_STORE = Chroma(persist_directory=VECTOR_DB_DIR, embedding_function=EMBEDDINGS)

# --- RAG Tool List (Exported for agent.py) ---
ALL_RAG_TOOLS = [] 

@tool
def query_paper_for_answer(user_query: str, paper_title_keywords: str) -> str:
    """
    Analyzes the content of a specific local paper to answer a question or provide 
    a detailed summary using RAG. Returns the answer and explicitly cites the source PDF.
    """
    retriever = VECTOR_STORE.as_retriever(
        search_kwargs={"k": 3, "filter": {"title": paper_title_keywords}}
    )
    relevant_docs = retriever.invoke(f"{user_query} about the paper titled: {paper_title_keywords}")
    
    if not relevant_docs:
        return f"Could not find relevant text chunks for the paper: {paper_title_keywords}."

    # 1. Collect unique source metadata from retrieved chunks
    source_metadata = set()
    for doc in relevant_docs:
        source_metadata.add((doc.metadata.get('title'), doc.metadata.get('source')))
        
    source_list = "\n".join([f"- Title: {t}, Source Path: {s}" for t, s in source_metadata])
    context = "\n---\n".join([doc.page_content for doc in relevant_docs])

    # 2. Use LLM to synthesize the answer and cite the source
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1, api_key=GEMINI_API_KEY)
    
    rag_prompt = f"""
    You are a research analyst. Use the CONTEXT provided below to answer the USER QUESTION.
    SYNTHESIZED ANSWER:
    """
    answer = llm.invoke(rag_prompt + context + f"\n\nUSER QUESTION: {user_query}").content
    
    # Append the sources explicitly to the final string output
    return f"{answer}\n\n--- SOURCES USED ---\n{source_list}"

@tool
def generate_research_ideas(paper_title_keywords: str, num_ideas: int = 3) -> str:
    """
    Analyzes the paper's content using RAG to identify limitations or future work 
    sections and generates novel research questions based on them.
    
    Args:
        paper_title_keywords: Keywords to identify the paper (e.g., "regression algorithms").
        num_ideas: The number of research ideas to generate (default is 3).
        
    Returns:
        A list of generated research ideas.
    """
    # 1. Retrieve sections relevant to limitations and future work
    retriever = VECTOR_STORE.as_retriever(
        search_kwargs={"k": 5, "filter": {"title": paper_title_keywords}}
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

ALL_RAG_TOOLS.append(query_paper_for_answer)
ALL_RAG_TOOLS.append(generate_research_ideas)

