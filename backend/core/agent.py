# core/agent.py
import os
from langgraph.graph import StateGraph, START, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import ToolMessage, AIMessage
from typing import List
from dotenv import load_dotenv

from core.graph_state import AgentState
from tools.data_utils import load_paper_data # Assuming this loads global PAPER_DF

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# ------------------------------------
# 1. NODE DEFINITIONS
# ------------------------------------

def create_langgraph_nodes(all_tools):
    """Defines the functions for the graph nodes (LLM call and Tool call)."""
    
    # Initialize LLM and bind tools
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.1, api_key=GEMINI_API_KEY)
    model_with_tools = llm.bind_tools(all_tools)
    tools_by_name = {tool.name: tool for tool in all_tools}

    def call_model(state: AgentState):
        """Node 1: Calls the LLM (Gemini) with history to decide the next step."""
        response = model_with_tools.invoke(state["messages"])
        return {"messages": [response]}

    def call_tool(state: AgentState):
        """Node 2: Executes the tool suggested by the LLM."""
        last_message = state["messages"][-1]
        tool_calls = last_message.tool_calls
        
        tool_results = []
        for call in tool_calls:
            tool_name = call['name']
            tool_args = call['args']
            
            print(f"\n--- TOOL CALL: {tool_name}({tool_args}) ---")
            
            tool_output = tools_by_name[tool_name].invoke(tool_args)
            
            # If output is a Pydantic object, convert it to a structured JSON string
            if hasattr(tool_output, 'model_dump_json'):
                output_content = tool_output.model_dump_json(indent=2)
            else:
                output_content = str(tool_output)
            
            tool_results.append(ToolMessage(
                content=output_content, 
                tool_call_id=call['id']
            ))
            
        return {"messages": tool_results}

    return call_model, call_tool

def route_decision(state: AgentState):
    """Conditional Edge: Decides whether to continue the loop or finish."""
    last_message = state["messages"][-1]
    if last_message.tool_calls:
        return "continue"
    else:
        return "end"

# ------------------------------------
# 2. GRAPH COMPILATION
# ------------------------------------

def create_research_langgraph(llm_executor_tools: List):
    """Builds and compiles the full LangGraph agent workflow."""
    
    # Load data needed by tools globally
    load_paper_data()
    
    # Define nodes and router
    call_model, call_tool = create_langgraph_nodes(llm_executor_tools)

    workflow = StateGraph(AgentState)

    workflow.add_node("llm", call_model)
    workflow.add_node("tool", call_tool)
    
    workflow.set_entry_point("llm")

    workflow.add_conditional_edges(
        "llm", 
        route_decision, 
        {"continue": "tool", "end": END}
    )
    
    workflow.add_edge("tool", "llm")

    app = workflow.compile()
    
    return app