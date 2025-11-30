# core/graph_state.py
from typing import TypedDict, Annotated, List, Optional
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field

# --- LangGraph State ---
class AgentState(TypedDict):
    """Represents the state of the agent's workflow."""
    messages: Annotated[List[BaseMessage], add_messages] 

# --- Pydantic Schemas for Structured Output ---

class Work(BaseModel):
    """Represents a single academic work with essential details and identifiers."""
    title: str = Field(..., description="The full title of the academic paper.")
    authors: str = Field(..., description="Comma-separated list of primary authors.")
    publication_year: int = Field(..., description="The year the work was published.")
    openalex_id: Optional[str] = Field(None, description="The OpenAlex identifier (W...) for the work.")
    doi: Optional[str] = Field(None, description="The Digital Object Identifier for the work.")

class CitedWorksList(BaseModel):
    """A structured list of works that cite the target paper."""
    citing_works: List[Work] = Field(..., description="A list of papers that reference the original work.")

class ResearchIdea(BaseModel):
    """A single, novel research question based on the paper's limitations."""
    research_question: str = Field(..., description="A specific, actionable research question.")
    justification: str = Field(..., description="The gap or limitation in the original paper this idea addresses.")
    
class ResearchIdeas(BaseModel):
    """A structured list of generated research ideas."""
    ideas: List[ResearchIdea] = Field(..., description="A collection of novel research ideas.")

class FinalResearchResponse(BaseModel):
    """The final structured output for the API response."""
    summary_of_findings: str = Field(description="A concise, human-readable summary of the tool results.")
    data_source_used: str = Field(description="The primary source used (e.g., 'Local PDF RAG', 'OpenAlex API', 'Both').")