# api_service.py
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from langchain_core.messages import HumanMessage, AIMessage
from dotenv import load_dotenv
import os
import shutil
import uvicorn

# Import your compiled LangGraph components and tools
from core.agent import create_research_langgraph
from tools.openalex_tools import ALL_OPENALEX_TOOLS
from tools.rag_tools import ALL_RAG_TOOLS
from api.document_manager import (
    process_and_index_pdf,
    list_documents,
    delete_document,
    generate_doc_id,
    UPLOAD_DIR
)

# --- CONFIGURATION ---
load_dotenv()
ALL_TOOLS = ALL_OPENALEX_TOOLS + ALL_RAG_TOOLS

app = FastAPI(title="Research Agent API")

# 1. CORS Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], # Restrict this to your frontend domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Agent Initialization
# Compile the graph once at API startup
try:
    AGENT_APP = create_research_langgraph(ALL_TOOLS)
except Exception as e:
    print(f"FATAL: Could not initialize LangGraph agent: {e}")
    AGENT_APP = None

@app.get("/health")
def health_check():
    return {"status": "ok", "agent_ready": AGENT_APP is not None}

@app.post("/api/chat/{session_id}")
async def chat_stream(session_id: str, prompt: str):
    """Exposes the agent execution loop via a streaming API endpoint (SSE)."""
    if not AGENT_APP:
        raise HTTPException(status_code=503, detail="Agent is not initialized.")
        
    inputs = {"messages": [HumanMessage(content=prompt)]}
    config = {"configurable": {"thread_id": session_id}}

    async def event_generator():
        # This function runs synchronously but yields control to FastAPI
        final_answer = ""
        
        try:
            for chunk in AGENT_APP.stream(inputs, config=config):
                # LangGraph streams dictionary deltas. We only care about the final message content.
                if "__end__" in chunk and chunk['__end__'] is not None:
                    final_messages = chunk['__end__']['messages']
                    if final_messages and isinstance(final_messages[-1], AIMessage):
                        content = final_messages[-1].content
                        
                        # Yield the data as Server-Sent Event (SSE)
                        yield f"data: {content}\n\n"
                        final_answer += content
            
            # Send the final DONE signal
            yield "data: [DONE]\n\n"

        except Exception as e:
            # Handle execution errors gracefully
            print(f"Agent stream error: {e}")
            yield f"data: [ERROR] An internal agent error occurred: {e}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/api/documents/upload")
async def upload_document(file: UploadFile = File(...)):
    """Upload and index a PDF document."""
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")
    
    try:
        # Generate unique ID for the document
        doc_id = generate_doc_id()
        file_path = os.path.join(UPLOAD_DIR, f"{doc_id}_{file.filename}")
        
        # Save the uploaded file
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Process and index the PDF
        result = process_and_index_pdf(file_path, file.filename, doc_id)
        
        return JSONResponse(content=result, status_code=201)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/documents")
def get_documents():
    """Get a list of all indexed documents."""
    try:
        documents = list_documents()
        return JSONResponse(content={"documents": documents})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/documents/{doc_id}")
def remove_document(doc_id: str):
    """Delete a document from the vector store."""
    try:
        success = delete_document(doc_id)
        if success:
            return JSONResponse(content={"message": "Document deleted successfully"})
        else:
            raise HTTPException(status_code=404, detail="Document not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Command to run the service: uvicorn api_service:app --reload
    print("Starting FastAPI service on http://127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)