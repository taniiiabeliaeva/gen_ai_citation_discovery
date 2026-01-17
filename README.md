# Gen AI Citation Discovery

A research assistant application that helps users discover relevant academic papers, analyze document content, and identify research gaps using AI-powered tools.

## Overview

This application combines a React frontend with a FastAPI backend powered by LangGraph agents. It enables researchers to:
- Upload and index PDF documents
- Ask questions about document content using RAG (Retrieval-Augmented Generation)
- Discover papers related to specific topics
- Find citations and related works via OpenAlex API
- Identify research gaps and contradictions

## Current Workflow

### 1. **Document Upload & Indexing**
- Users upload PDF documents through the Document Explorer sidebar
- Documents are automatically indexed into a FAISS vector database
- Each document is chunked and embedded for semantic search

### 2. **Topic Discovery**
**User asks:** "Which papers are relevant for [X topic]?"

**Agent responds with:**
- List of relevant papers from the indexed documents
- Papers are identified using semantic search across all uploaded PDFs
- Results include paper titles, file paths, and relevance scores

### 3. **Paper Inspection**
Users can:
- **View PDFs** - Click on any document to open the PDF viewer
- **Read content** - Navigate through pages with built-in controls
- **Select papers** - Mark papers for focused questioning

### 4. **Deep Analysis**
Users can ask:
- **General questions**: "What is Spectre?" → Uses `answer_general_question` tool
- **Specific paper questions**: "What does the Fix Spectre paper say about hardware mitigations?" → Uses `query_paper_for_answer` tool
- **Research ideas**: "What are the limitations of this approach?" → Uses `generate_research_ideas` tool
- **Citation searches**: "What papers cite this work?" → Uses OpenAlex tools

## Current Features

### ✅ Implemented

#### Backend (FastAPI + LangGraph)
- **Agent System**: LangGraph-based agent with tool calling
- **RAG Tools**:
  - `answer_general_question` - Search across all documents
  - `query_paper_for_answer` - Query specific papers by file path
  - `generate_research_ideas` - Identify limitations and suggest future work
  - `recommend_relevant_papers` – Suggest papers relevant to a topic
  - `identify_research_gaps` – Analyze multiple papers to find research gaps
  - `compare_papers` – Perform comparative analysis of papers on specific aspects
  - `verify_claim` – Check if a claim is supported, contradicted, or not addressed by indexed papers
- **OpenAlex Tools**:
  - Search for papers by topic
  - Find citing papers
  - Get paper metadata
- **Document Management**:
  - Upload PDFs
  - List indexed documents
  - Delete documents
  - Serve PDF files
- **Vector Database**: FAISS for semantic search
- **Logging**: Comprehensive logging for debugging agent decisions

#### Frontend (React + Vite)
- **Chat Interface**: Streaming responses from the agent
- **Document Explorer**: Browse and manage uploaded documents
- **PDF Viewer**: react-pdf based viewer with page navigation
- **Three-Panel Layout**: Documents, PDF viewer, and chat

## Architecture

```
┌─────────────────┐
│   Frontend      │
│   (React)       │
│                 │
│  - Chat UI      │
│  - Doc Explorer │
│  - PDF Viewer   │
└────────┬────────┘
         │ HTTP/SSE
         ▼
┌─────────────────┐
│   Backend       │
│   (FastAPI)     │
│                 │
│  - API Routes   │
│  - Agent System │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│   LangGraph     │      │   Vector DB  │
│   Agent         │◄────►│   (FAISS)    │
│                 │      └──────────────┘
│  - LLM Node     │
│  - Tool Node    │      ┌──────────────┐
│  - Router       │◄────►│  OpenAlex    │
└─────────────────┘      │  API         │
                         └──────────────┘
```

## Roadmap

### Phase 1: Enhanced Paper Discovery & Flagging 🎯

#### 1.1 Paper Recommendation Tool
**Tool**: `recommend_relevant_papers`
- **Purpose**: Identify papers relevant to a specific research topic
- **Input**: Topic/query string, number of papers to return
- **Output**: Ranked list of papers with relevance scores
- **Implementation**:
  - Use semantic search across vector database
  - Return paper metadata (title, file_path, relevance_score)
  - Agent can present results to user

#### 1.2 Frontend Paper Flagging
**Feature**: Visual indicators for recommended papers
- **UI Changes**:
  - Add "relevance score" badge to document cards
  - Color-code documents: 🟢 High relevance, 🟡 Medium, ⚪ Low
  - Add "Sort by relevance" option in Document Explorer
  - Pin highly relevant papers to top of list
- **State Management**:
  - Store relevance scores in component state
  - Update when agent returns recommendations
  - Persist during session

#### 1.3 Agent Integration
- Agent returns structured response with paper recommendations
- Frontend parses response and updates document list
- User can click flagged papers to view/analyze

---

### Phase 2: Add Tools 🔍

#### 2.1 Gap Detection Tool
**Tool**: `identify_research_gaps`
- **Purpose**: Analyze multiple papers to find unexplored areas
- **Input**: List of paper file paths or topic
- **Output**: Structured list of research gaps with justifications
- **Implementation**:
  - Retrieve content from multiple papers
  - Use LLM to identify:
    - Limitations mentioned across papers
    - Questions raised but not answered
    - Methodologies not yet applied
    - Contradictions or disagreements
  - Return structured output with evidence citations

#### 2.2 Comparative Analysis Tool
**Tool**: `compare_papers`
- **Purpose**: Compare approaches, findings, or methodologies across papers
- **Input**: List of paper file paths, comparison aspect (e.g., "methodology", "findings")
- **Output**: Structured comparison table
- **Implementation**:
  - Extract relevant sections from each paper
  - Use LLM to create structured comparison
  - Highlight similarities and differences

#### 2.3 Claim Verification Tool
**Tool**: `verify_claim`
- **Purpose**: Check if a specific claim is supported or contradicted by other papers
- **Input**: Claim statement, optional paper context
- **Output**: Supporting/contradicting evidence from indexed papers
- **Implementation**:
  - Search for relevant passages about the claim
  - Classify as supporting, contradicting, or neutral
  - Return evidence with citations

---

### Phase 3: Enhanced User Experience 🎨

#### 3.1 Frontend Enhancements
- **Contradiction Highlighting**:
  - Show contradictions in chat with expandable details
  - Link to specific papers and page numbers
  - Visual indicators for conflicting papers

#### 3.2 Export & Reporting
- Export research gaps as markdown
- Generate literature review summaries
- Create citation networks visualization

---

### Phase 4: Advanced Features 🚀

#### 4.1 Citation Network Analysis
**Tool**: `analyze_citation_network`
- Build citation graphs using OpenAlex
- Identify influential papers
- Find citation clusters

#### 4.2 Multi-Document Synthesis
**Tool**: `synthesize_literature`
- Generate comprehensive literature reviews
- Combine findings from multiple papers
- Create structured summaries by theme

---

## Getting Started

**Note:** This project uses Git Large File Storage (LFS) for FAISS index files. Please ensure Git LFS is installed before cloning or pulling the repository

### 1. Data setup
Please download the full extracted PDF data archive from Google Drive: https://drive.google.com/drive/folders/1kbam5je_CuscAuGoF3IcZZ7sRv8FLCJu?usp=sharing and unzip it into `backend/data/pdfs`.

**Important:** After cloning or pulling the repository, verify that `backend/data/faiss_db_gemini` and `backend/data/faiss_db_mistral` are not empty and contain the expected files. Git LFS budget quota limits may prevent these files from downloading correctly.
If this happens, restore your working tree and pull the repository without downloading LFS files:

```bash
git restore .
GIT_LFS_SKIP_SMUDGE=1 git pull
```

Then download the FAISS data manually and place it into the corresponding directories.

### 2. Environment Variables
Create `.env` in backend directory:
```
GOOGLE_API_KEY=your_google_api_key_here
AQUEDUCT_API_KEY=your_tuwien_aqueduct_api_key_here
```

**Note**: 
- `GOOGLE_API_KEY` is required for Gemini text generation models
- `AQUEDUCT_API_KEY` is required for TU Wien models (GLM-4.6 text generation + Mistral embeddings)

### 3. Backend Setup
```bash
cd backend
uv sync
uv run main.py
```

### 4. Frontend Setup
```bash
cd frontend
npm install
npm run dev
```
---

## Technical Requirements

### New Tools to Implement

Each tool should follow this pattern:

```python
@tool
def tool_name(param1: str, param2: int = 5) -> str:
    """
    Clear description of what the tool does.
    
    Args:
        param1: Description of parameter
        param2: Description with default value
    
    Returns:
        Description of return value
    """
    # Implementation
    pass
```

When adding new tools:
1. Define the tool in `backend/tools/` directory
2. Add to appropriate tool list (RAG_TOOLS or OPENALEX_TOOLS)
3. Update system prompt in `backend/core/agent.py`
4. Add logging for debugging
5. Test with sample queries

### Agent System Prompt Updates
- Add guidance for when to use each new tool
- Update workflow instructions
- Provide examples of tool combinations

### Frontend State Management
- Add relevance scores to document state
- Store research gaps in session state
- Manage contradiction highlights
