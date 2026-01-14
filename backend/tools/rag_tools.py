# tools/rag_tools.py
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.language_models.chat_models import BaseChatModel
from llm.model import LanguageModel, get_model_instance
from tools.data_utils import _get_openalex_id_from_title
from core.graph_state import ResearchIdeas  # Import Pydantic models TODO
from vectordb_utils.document_manager import DocumentManager
from dotenv import load_dotenv
import os

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

_DOCUMENT_MANAGER: DocumentManager = None
_LLM_INSTANCE: BaseChatModel = None


def tools_set_document_manager(document_manager: DocumentManager) -> None:
    global _DOCUMENT_MANAGER
    _DOCUMENT_MANAGER = document_manager

def tools_set_llm(model: LanguageModel) -> None:
    global _LLM_INSTANCE
    _LLM_INSTANCE = get_model_instance(model)

def _get_vector_store():
    global _DOCUMENT_MANAGER
    if _DOCUMENT_MANAGER is None:
        raise ValueError("DocumentManager is not set. Please initialize it first.")
    return _DOCUMENT_MANAGER.vector_store


def _canonicalize_file_path(file_path: str) -> str:
    global _DOCUMENT_MANAGER
    if _DOCUMENT_MANAGER is None:
        raise ValueError("DocumentManager is not set. Please initialize it first.")
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

    rag_prompt = f"""
    You are a research analyst. Use the CONTEXT provided below to answer the USER QUESTION.
    SYNTHESIZED ANSWER:
    """
    answer = _LLM_INSTANCE.invoke(
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
    response = _LLM_INSTANCE.invoke(generator_prompt)
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

    answer = _LLM_INSTANCE.invoke(rag_prompt).content

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

    # Use similarity search with scores (FAISS IndexFlatL2 → lower distance = more similar)
    results = _get_vector_store().similarity_search_with_score(topic, k=num_papers)

    if not results:
        return "No papers found matching the topic. Please try a different query or upload more documents."

    print(f"[RECOMMEND TOOL] Found {len(results)} relevant chunks")

    # Extract unique papers, keeping best (lowest) distance per paper
    papers_dict = {}
    for doc, distance in results:
        file_path = doc.metadata.get("file_path") or doc.metadata.get("source")
        if file_path:
            file_path = _canonicalize_file_path(file_path)
        title = doc.metadata.get("title", "Untitled")

        if not file_path:
            continue

        existing = papers_dict.get(file_path)
        if existing is None or distance < existing["_distance"]:
            papers_dict[file_path] = {
                "title": title,
                "file_path": file_path,
                "_distance": float(distance),
            }

    # Convert distances to relevance scores using inverse method
    # Distance 0 → score 1.0, larger distances → score approaches 0 (but never reaches it)
    for paper in papers_dict.values():
        score = 1 / (1 + paper["_distance"])
        paper["relevance_score"] = round(float(score), 3)
        paper.pop("_distance", None)

    # Convert to list and sort by relevance score (higher is better)
    papers_list = list(papers_dict.values())
    papers_list.sort(key=lambda x: x["relevance_score"], reverse=True)

    # Format response
    import json

    result = {"topic": topic, "num_results": len(papers_list), "papers": papers_list}

    print(f"[RECOMMEND TOOL] Returning {len(papers_list)} unique papers")
    return json.dumps(result, indent=2)

@tool
def identify_research_gaps(topic: str, file_paths: str = None) -> str:
    """
    Analyzes multiple papers to identify unexplored areas, contradictions, and common limitations.
    
    Use this tool when the user asks:
    - "What is missing in this field?"
    - "Find research gaps in these papers"
    - "What are the open problems?"

    Args:
        topic: The research topic (e.g., "LLM hallucinations").
        file_paths: (Optional) A comma-separated string of file paths to restrict the analysis to specific papers.

    Returns:
        A structured analysis of research gaps with citations.
    """
    print(f"\n[GAP ANALYSIS TOOL] Analyzing gaps for topic: '{topic}'")
    
    # finding sections discussing weaknesses
    # keywords that usually appear in "Future Work" or "Discussion" sections
    search_query = f"{topic} limitations future work challenges open problems conclusion"
    
    relevant_docs = []

    # MODE 1: Specific Files Selected
    if file_paths:
        # Split the string "path1.pdf, paper2.pdf" into a clean list
        paths_list = [p.strip() for p in file_paths.split(',') if p.strip()]
        print(f"[GAP ANALYSIS TOOL] Restricting search to {len(paths_list)} specific files.")
        
        for path in paths_list:
            # Canonicalize path before searching
            canonical_path = _canonicalize_file_path(path)
            # We search EACH paper individually for its limitations/conclusions
            retriever = _get_vector_store().as_retriever(
                search_kwargs={"k": 5, "filter": {"file_path": canonical_path}}
            )
            docs = retriever.invoke(search_query)
            relevant_docs.extend(docs)
            
    # MODE 2: General Topic Search (Fallback if no papers selected)
    else:
        print(f"[GAP ANALYSIS TOOL] Searching entire database (no files selected).")
        retriever = _get_vector_store().as_retriever(search_kwargs={"k": 15})
        relevant_docs = retriever.invoke(search_query)

    if not relevant_docs:
        return "I couldn't find enough information on limitations or future work in the selected documents. They might not contain explicit 'Future Work' sections."

    # 2. Extract context and source metadata
    context_parts = []
    seen_sources = set()
    
    for doc in relevant_docs:
        title = doc.metadata.get("title", "Untitled")
        content = doc.page_content
        # Create a unique key to avoid citing the exact same paper/chunk multiple times
        source_key = f"{title}-{content[:50]}"
        
        if source_key not in seen_sources:
            context_parts.append(f"Source: {title}\nExcerpt: {content}")
            seen_sources.add(source_key)

    context_str = "\n\n---\n\n".join(context_parts)

    # 3. LLM Synthesis
    gap_analysis_prompt = f"""
    You are a senior academic researcher conducting a literature review. 
    Analyze the provided EXCERPTS from various papers regarding the topic: '{topic}'.

    Your goal is to identify **Research Gaps**. Look for:
    1. **Common Limitations:** Problems that multiple papers mention but haven't solved.
    2. **Contradictions:** Areas where Paper A says one thing and Paper B says another.
    3. **Underexplored Areas:** Methodologies or questions that are mentioned as "future work".

    CONTEXT EXCERPTS:
    {context_str}

    INSTRUCTIONS:
    - Output a structured response with clear headings (e.g., "## 1. Unsolved Limitations").
    - **Crucially**: specificy WHICH papers support this gap (e.g., "Paper A and Paper B both struggle with high latency...").
    - If the papers do not reveal clear gaps, admit it honestly rather than hallucinating one.
    """

    response = _LLM_INSTANCE.invoke(gap_analysis_prompt)
    return response.content

@tool
def compare_papers(
    topic: str,
    file_paths: str = None,
    aspect: str = "methodology, findings, and limitations"
) -> str:
    """
    Compares multiple academic papers with respect to a given aspect.

    Use this tool when the user asks:
    - "Compare these papers"
    - "How do the approaches differ?"
    - "Contrast the findings of these works"

    Args:
        topic: The topic guiding the comparison.
        file_paths: Optional comma-separated list of paper file paths.
        aspect: Aspect to compare (default: methodology, findings, limitations).

    Returns:
        A structured comparative analysis.
    """

    print(f"\n[COMPARISON TOOL] Topic: '{topic}', Aspect: '{aspect}'")

    selected_docs = []

    # Use a BROAD retriever — comparison needs coverage, not precision
    retriever = _get_vector_store().as_retriever(search_kwargs={"k": 10})

    # Case 1: User selected specific papers
    if file_paths:
        paths_list = [p.strip() for p in file_paths.split(",") if p.strip()]
        print(f"[COMPARISON TOOL] Comparing specific files: {paths_list}")

        indexed_paths = set()
        if _DOCUMENT_MANAGER is not None:
            indexed_paths = {
                d.get("file_path", "") for d in _DOCUMENT_MANAGER.list_documents() if d.get("file_path", "")
            }

        resolved_paths = []
        missing_paths = []
        for path in paths_list:
            canonical_path = _canonicalize_file_path(path)
            candidate_paths = [canonical_path]
            if canonical_path and not canonical_path.startswith("pdfs/"):
                candidate_paths.append(_canonicalize_file_path(f"pdfs/{path}"))

            resolved = next((p for p in candidate_paths if p in indexed_paths), None)
            if not resolved:
                missing_paths.append(path)
            else:
                resolved_paths.append((path, resolved))

        if missing_paths:
            missing_str = ", ".join(missing_paths)
            raise ValueError(
                f"The following file_paths are not indexed and cannot be compared: {missing_str}. "
                f"Upload/index them first."
            )

        retrieval_query = f"{topic}\n\nFocus on: {aspect}"
        for original_path, canonical_path in resolved_paths:
            per_paper_retriever = _get_vector_store().as_retriever(
                search_kwargs={"k": 10, "filter": {"file_path": canonical_path}}
            )
            docs = per_paper_retriever.invoke(retrieval_query)
            print(f"[DEBUG] Retrieved {len(docs)} chunks for {original_path} (canonical: {canonical_path})")

            if not docs:
                selected_docs.append(
                    f"PAPER: {original_path} ({canonical_path})\nCONTENT:\n(No relevant chunks retrieved)"
                )
                continue

            content = "\n".join([d.page_content for d in docs[:5]])
            title = docs[0].metadata.get("title") if docs else None
            paper_label = f"{title} ({canonical_path})" if title else f"{original_path} ({canonical_path})"
            selected_docs.append(
                f"PAPER: {paper_label}\nCONTENT:\n{content}"
            )

    # Case 2: No specific papers → compare most relevant ones
    else:
        print("[COMPARISON TOOL] Finding relevant papers automatically")

        docs = retriever.invoke(topic)

        grouped = {}
        for doc in docs:
            source = doc.metadata.get("title") or doc.metadata.get("source", "Unknown Paper")
            grouped.setdefault(source, []).append(doc.page_content)

        for source, contents in list(grouped.items())[:3]:
            content = "\n".join(contents[:5])
            selected_docs.append(
                f"PAPER: {source}\nCONTENT:\n{content}"
            )

    if not selected_docs:
        return "Not enough information found to compare the papers."

    context_str = "\n\n====================\n\n".join(selected_docs)

    # LLM: low temperature for factual comparison
    llm = _LLM_INSTANCE
    if llm is None:
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0.1
        )

    comparison_prompt = f"""
You are an academic research assistant.

Compare the following papers with respect to:
"{aspect}"

CONTEXT:
{context_str}

INSTRUCTIONS:
1. Summarize each paper's approach related to '{aspect}'.
2. Identify similarities between the papers.
3. Identify key differences.
4. Present the result clearly.
5. If information is missing, state this explicitly.

OUTPUT FORMAT:

## Comparative Analysis: {aspect}

### Paper Summaries
- Paper A: ...
- Paper B: ...

### Similarities
- ...

### Differences
- ...

### Comparison Table
| Paper | Approach | Key Findings | Limitations |
|------|----------|--------------|-------------|
| ...  | ...      | ...          | ...         |
"""

    response = llm.invoke(comparison_prompt)
    return response.content


ALL_RAG_TOOLS.append(query_paper_for_answer)
ALL_RAG_TOOLS.append(generate_research_ideas)
ALL_RAG_TOOLS.append(answer_general_question)
ALL_RAG_TOOLS.append(recommend_relevant_papers)
ALL_RAG_TOOLS.append(identify_research_gaps)
ALL_RAG_TOOLS.append(compare_papers)
