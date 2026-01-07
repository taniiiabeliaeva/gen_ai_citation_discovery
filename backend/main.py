# api_service.py
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from dotenv import load_dotenv
import os
import shutil
import uvicorn

# Import your compiled LangGraph components and tools
from core.agent import create_research_langgraph
from tools.openalex_tools import ALL_OPENALEX_TOOLS
from tools.rag_tools import ALL_RAG_TOOLS
from vectordb_utils.document_manager import DocumentManager

# --- CONFIGURATION ---
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
ALL_TOOLS = ALL_OPENALEX_TOOLS + ALL_RAG_TOOLS


# Ensure upload directory exists
UPLOAD_DIR = "data/pdfs"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(title="Research Agent API")

# 1. CORS Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000"
    ],  # Restrict this to your frontend domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Agent Initialization
# Compile the graph once at API startup
try:
    AGENT_APP = create_research_langgraph(ALL_TOOLS, GOOGLE_API_KEY)
except Exception as e:
    print(f"FATAL: Could not initialize LangGraph agent: {e}")
    AGENT_APP = None

document_manager = DocumentManager(GOOGLE_API_KEY)


@app.get("/health")
def health_check():
    return {"status": "ok", "agent_ready": AGENT_APP is not None}


@app.post("/api/chat/{session_id}")
async def chat_stream(session_id: str, prompt: str):
    """Exposes the agent execution loop via a streaming API endpoint (SSE)."""
    print(f"\n{'#'*70}")
    print(f"[API REQUEST] POST /api/chat/{session_id}")
    print(f"[API REQUEST] Prompt: {prompt[:100]}{'...' if len(prompt) > 100 else ''}")
    print(f"{'#'*70}")

    if not AGENT_APP:
        raise HTTPException(status_code=503, detail="Agent is not initialized.")

    inputs = {"messages": [HumanMessage(content=prompt)]}
    config = {"configurable": {"thread_id": session_id}}

    async def event_generator():
        # This function runs synchronously but yields control to FastAPI
        final_answer = ""
        chunk_count = 0
        tool_metadata = {}  # Track tool calls and their results

        try:
            print(f"\n[STREAMING] Starting agent stream for session: {session_id}")

            for chunk in AGENT_APP.stream(inputs, config=config):
                chunk_count += 1
                print(f"[STREAMING] Received chunk #{chunk_count} from agent")
                print(f"[STREAMING DEBUG] Chunk keys: {chunk.keys()}")

                # LangGraph streams chunks with node names as keys
                # We need to look for chunks from the 'agent' or final node
                for node_name, node_data in chunk.items():

                    # Check if this chunk contains messages
                    if isinstance(node_data, dict) and "messages" in node_data:
                        messages = node_data["messages"]

                        # Process each message in the chunk
                        for message in (
                            messages if isinstance(messages, list) else [messages]
                        ):
                            # Detect tool calls and results
                            if isinstance(message, ToolMessage):
                                # Check if this is from recommend_relevant_papers
                                try:
                                    import json

                                    tool_result = json.loads(message.content)
                                    if (
                                        "topic" in tool_result
                                        and "papers" in tool_result
                                    ):
                                        print(
                                            f"[STREAMING] Detected recommendation tool result"
                                        )
                                        # Extract scores for metadata
                                        scores = {}
                                        for paper in tool_result.get("papers", []):
                                            if (
                                                "file_path" in paper
                                                and "relevance_score" in paper
                                            ):
                                                scores[paper["file_path"]] = paper[
                                                    "relevance_score"
                                                ]

                                        tool_metadata["recommendations"] = {
                                            "topic": tool_result.get("topic"),
                                            "scores": scores,
                                            "num_results": tool_result.get(
                                                "num_results", len(scores)
                                            ),
                                        }
                                        print(
                                            f"[STREAMING] Stored metadata for {len(scores)} papers"
                                        )
                                except (json.JSONDecodeError, Exception) as e:
                                    # Not a JSON tool result or not recommendation data
                                    pass

                            if isinstance(message, AIMessage):
                                content = message.content

                                # Handle new Gemini API format where content is a list of dicts
                                if isinstance(content, list):
                                    # Extract text from list of content blocks
                                    text_content = ""
                                    for block in content:
                                        if isinstance(block, dict) and "text" in block:
                                            text_content += block["text"]
                                    content = text_content

                                # Only send non-empty content
                                if content and isinstance(content, str):
                                    print(
                                        f"[STREAMING] Sending content to client (length: {len(content)} chars)"
                                    )
                                    print(
                                        f"[STREAMING DEBUG] Content preview: {content[:100]}..."
                                    )

                                    # Yield the data as Server-Sent Event (SSE)
                                    yield f"data: {content}\n\n"
                                    final_answer = content  # Keep the latest answer

            # Send metadata event if we detected recommendation tool usage
            if tool_metadata.get("recommendations"):
                import json

                print(f"[STREAMING] Sending metadata event with recommendations")
                metadata_json = json.dumps(tool_metadata)
                yield f"event: metadata\ndata: {metadata_json}\n\n"

            # Send the final DONE signal
            print(f"[STREAMING] Stream complete - sending [DONE] signal")
            print(
                f"######################################################################\n"
            )
            yield "data: [DONE]\n\n"

        except Exception as e:
            # Handle execution errors gracefully
            print(f"[STREAMING ERROR] Agent stream error: {e}")
            print(
                f"######################################################################\n"
            )
            yield f"data: [ERROR] An internal agent error occurred: {e}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/documents/upload")
async def upload_document(file: UploadFile = File(...)):
    """Upload and index a PDF document."""
    print(f"\n[API REQUEST] POST /api/documents/upload")
    print(f"[API REQUEST] Filename: {file.filename}")

    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")

    try:
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        print(f"[UPLOAD] Saving file to: {file_path}")

        # Save the uploaded file
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        print(f"[UPLOAD] File saved successfully, starting indexing...")

        # Process and index the PDF
        result = document_manager.process_and_index_pdf(
            file_path, None
        )  # TODO add metadata

        print(f"[UPLOAD] Document indexed successfully: {result}")

        return JSONResponse(content=result, status_code=201)
    except Exception as e:
        print(f"[UPLOAD ERROR] Failed to upload document: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/documents")
def get_documents():
    """Get a list of all indexed documents."""
    print(f"\n[API REQUEST] GET /api/documents")
    try:
        documents = document_manager.list_documents()
        print(f"[API RESPONSE] Returning {len(documents)} document(s)")
        return JSONResponse(content={"documents": documents})
    except Exception as e:
        print(f"[API ERROR] Failed to list documents: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/documents/{doc_id}")
def remove_document(doc_id: str):
    """Delete a document from the vector store."""
    print(f"\n[API REQUEST] DELETE /api/documents/{doc_id}")
    try:
        success = document_manager.delete_document(doc_id)
        if success:
            print(f"[API RESPONSE] Document {doc_id} deleted successfully")
            return JSONResponse(content={"message": "Document deleted successfully"})
        else:
            print(f"[API ERROR] Document {doc_id} not found")
            raise HTTPException(status_code=404, detail="Document not found")
    except Exception as e:
        print(f"[API ERROR] Failed to delete document: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/documents/pdf/{file_path:path}")
def get_pdf(file_path: str):
    """Serve a PDF file from the data directory."""
    print(f"\n[API REQUEST] GET /api/documents/pdf/{file_path}")
    try:
        print(f"[PDF SERVE]: {file_path}")

        # Check if file exists
        if not os.path.exists(file_path):
            print(f"[PDF SERVE ERROR] File not found: {file_path}")
            raise HTTPException(status_code=404, detail="PDF file not found")

        print(f"[PDF SERVE] Serving file: {os.path.basename(file_path)}")
        # Return the PDF file
        return FileResponse(
            file_path,
            media_type="application/pdf",
            filename=os.path.basename(file_path),
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"[PDF SERVE ERROR] Failed to serve PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # Command to run the service: uvicorn api_service:app --reload
    print("Starting FastAPI service on http://127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)
