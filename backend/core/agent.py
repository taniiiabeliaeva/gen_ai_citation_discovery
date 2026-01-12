# core/agent.py
import os
from langgraph.graph import StateGraph, START, END
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import ToolMessage, AIMessage
from typing import List
from dotenv import load_dotenv

from core.graph_state import AgentState
from tools.data_utils import load_paper_data  # Assuming this loads global PAPER_DF

# Load environment variables
load_dotenv()

# ------------------------------------
# SYSTEM PROMPT
# ------------------------------------

SYSTEM_PROMPT = """You are a research assistant helping users explore academic papers and citations.

IMPORTANT TOOL USAGE GUIDELINES:

1. **For discovering papers on a topic** (MOST COMMON for "which papers..." questions):
   - Use the `recommend_relevant_papers` tool
   - This returns a ranked list of papers with relevance scores
   - Perfect for "Which papers are relevant for X?" or "Find papers about Y"
   - Returns structured JSON that the frontend can use to highlight papers

2. **For identifying missing knowledge or open problems**:
   - Use the `identify_research_gaps` tool
   - Use this when the user asks "What is missing?", "What are the gaps?", "What are the limitations of this field?", or "Are there contradictions?"
   - This tool analyzes multiple papers to find common weaknesses and future work suggestions.

3. **For comparing multiple papers**:
   - Use the `compare_papers` tool 
   - Use this when the user asks "Compare Paper A and Paper B", "How do these papers differ?", or "Contrast the methodologies"
   - This tool creates a structured comparison highlighting similarities and differences.

4. **For general questions about any topic or documents**:
   - Use the `answer_general_question` tool as your fallback tool
   - This tool searches across ALL indexed documents and provides comprehensive answers with citations
   - Use this when the user asks general questions without specifying a particular paper
   - Example queries: "What is Spectre?", "Explain hardware vulnerabilities", "What do you know about X?"

5. **For questions about a SPECIFIC paper**:
   - Use the `query_paper_for_answer` tool when you know the exact paper file path
   - This tool filters results to a specific document
   - Use when the user says "In the paper titled X, what..." or "According to [paper name]..."

6. **For citation searches**:
   - Use OpenAlex tools to find papers that cite a specific work
   - Use when the user asks "what papers cite X?" or "find citations for Y"

7. **For generating research ideas, explaining limitations, and suggesting future work**:
   - Use the `generate_research_ideas` tool to identify limitations and suggest future work

WORKFLOW:
- If user asks "which papers are relevant for X?", use `recommend_relevant_papers`
- If user asks "What are the gaps..." -> `identify_research_gaps`
- If user asks "Compare these papers..." -> `compare_papers`
- If the question is general, use `answer_general_question`
- If the question is specific to a paper, use `query_paper_for_answer`
- If the question is about citations, use OpenAlex tools
- If the question is about generating research ideas, use `generate_research_ideas`
- Always provide comprehensive answers with proper citations from the retrieved sources

Remember: `recommend_relevant_papers` is your PRIMARY tool for paper discovery. `compare_papers` is for deep comparative analysis. `answer_general_question` is your fallback tool for answering questions about general content."""

# ------------------------------------
# 1. NODE DEFINITIONS
# ------------------------------------


def create_langgraph_nodes(all_tools, google_api_key: str):
    """Defines the functions for the graph nodes (LLM call and Tool call)."""

    # Initialize LLM and bind tools
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash", temperature=0.1, google_api_key=google_api_key
    )
    model_with_tools = llm.bind_tools(all_tools)
    tools_by_name = {tool.name: tool for tool in all_tools}

    def call_model(state: AgentState):
        """Node 1: Calls the LLM (Gemini) with history to decide the next step."""
        print(f"\n{'='*60}")
        print(
            f"[AGENT STEP] Calling LLM with {len(state['messages'])} message(s) in history"
        )
        print(f"{'='*60}")

        # Prepend system prompt to guide the agent's behavior
        from langchain_core.messages import SystemMessage

        messages_with_system = [SystemMessage(content=SYSTEM_PROMPT)] + state[
            "messages"
        ]

        response = model_with_tools.invoke(messages_with_system)

        print(f"\n[LLM RESPONSE] Received response from model")
        if hasattr(response, "tool_calls") and response.tool_calls:
            print(
                f"[LLM RESPONSE] Model requested {len(response.tool_calls)} tool call(s)"
            )
            for i, call in enumerate(response.tool_calls, 1):
                print(f"  Tool {i}: {call['name']}")
        else:
            print(f"[LLM RESPONSE] Model provided final answer (no tool calls)")
            if hasattr(response, "content"):
                content_preview = (
                    response.content[:100]
                    if len(response.content) > 100
                    else response.content
                )
                print(f"[LLM RESPONSE] Content preview: {content_preview}...")

        return {"messages": [response]}

    def call_tool(state: AgentState):
        """Node 2: Executes the tool suggested by the LLM."""
        last_message = state["messages"][-1]
        tool_calls = last_message.tool_calls

        print(f"\n{'='*60}")
        print(f"[AGENT STEP] Executing {len(tool_calls)} tool(s)")
        print(f"{'='*60}")

        tool_results = []
        for idx, call in enumerate(tool_calls, 1):
            tool_name = call["name"]
            tool_args = call["args"]

            print(f"\n[TOOL {idx}/{len(tool_calls)}] Calling: {tool_name}")
            print(f"[TOOL {idx}/{len(tool_calls)}] Arguments: {tool_args}")

            try:
                tool_output = tools_by_name[tool_name].invoke(tool_args)

                # If output is a Pydantic object, convert it to a structured JSON string
                if hasattr(tool_output, "model_dump_json"):
                    output_content = tool_output.model_dump_json(indent=2)
                else:
                    output_content = str(tool_output)

                output_preview = (
                    output_content[:200]
                    if len(output_content) > 200
                    else output_content
                )
                print(
                    f"[TOOL {idx}/{len(tool_calls)}] Result preview: {output_preview}..."
                )
                print(f"[TOOL {idx}/{len(tool_calls)}] ✓ Completed successfully")

                tool_results.append(
                    ToolMessage(content=output_content, tool_call_id=call["id"])
                )
            except Exception as e:
                print(f"[TOOL {idx}/{len(tool_calls)}] ✗ Error: {str(e)}")
                raise

        print(
            f"\n[AGENT STEP] All tools executed, returning {len(tool_results)} result(s) to LLM"
        )
        return {"messages": tool_results}

    return call_model, call_tool


def route_decision(state: AgentState):
    """Conditional Edge: Decides whether to continue the loop or finish."""
    last_message = state["messages"][-1]

    print(f"\n[ROUTING] Determining next step...")

    if last_message.tool_calls:
        print(
            f"[ROUTING] → Continuing to tool execution (found {len(last_message.tool_calls)} tool call(s))"
        )
        return "continue"
    else:
        print(f"[ROUTING] → Ending agent loop (no more tool calls)")
        print(f"{'='*60}")
        print(f"[AGENT] Workflow complete - returning final answer to user")
        print(f"{'='*60}\n")
        return "end"


# ------------------------------------
# 2. GRAPH COMPILATION
# ------------------------------------


def create_research_langgraph(llm_executor_tools: List, google_api_key: str):
    """Builds and compiles the full LangGraph agent workflow."""

    # Load data needed by tools globally
    load_paper_data()

    # Define nodes and router
    call_model, call_tool = create_langgraph_nodes(llm_executor_tools, google_api_key)

    workflow = StateGraph(AgentState)

    workflow.add_node("llm", call_model)
    workflow.add_node("tool", call_tool)

    workflow.set_entry_point("llm")

    workflow.add_conditional_edges(
        "llm", route_decision, {"continue": "tool", "end": END}
    )

    workflow.add_edge("tool", "llm")

    app = workflow.compile()

    return app
