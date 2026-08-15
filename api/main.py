import logging
import os
import sys
from contextlib import asynccontextmanager
from uuid import uuid4

# Ensure project root directory is in sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

try:
    from api.db import create_job, get_job, get_job_logs, init_db
    from api.kafka_producer import publish_message
    from api.models import ResearchRequest
    from agents.compiler.worker import register_compiler_worker
    from agents.critic.worker import register_critic_worker
    from agents.planner.worker import register_planner_worker
    from agents.searcher.worker import register_searcher_worker
    from agents.summarizer.worker import register_summarizer_worker
    from shared.kafka_config import REQUEST_TOPIC
    from status_relay.worker import manager as ws_manager, register_status_relay
except ImportError:
    from .db import create_job, get_job, get_job_logs, init_db
    from .kafka_producer import publish_message
    from .models import ResearchRequest
    from ..agents.compiler.worker import register_compiler_worker
    from ..agents.critic.worker import register_critic_worker
    from ..agents.planner.worker import register_planner_worker
    from ..agents.searcher.worker import register_searcher_worker
    from ..agents.summarizer.worker import register_summarizer_worker
    from ..shared.kafka_config import REQUEST_TOPIC
    from ..status_relay.worker import manager as ws_manager, register_status_relay

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup initialization
    logger.info("Initializing SQLite database...")
    init_db()

    logger.info("Registering Agent Pipeline Workers and Status Relay...")
    register_planner_worker()
    register_searcher_worker()
    register_summarizer_worker()
    register_critic_worker()
    register_compiler_worker()
    register_status_relay()

    yield
    logger.info("Shutting down API...")


app = FastAPI(title="StreamAgent AI API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "streamagent-ai-api"}


@app.post("/research")
def create_research_job(request: ResearchRequest):
    topic = request.topic.strip()
    if not topic:
        raise HTTPException(status_code=400, detail="topic is required")

    job_id = str(uuid4())
    job = create_job(job_id, topic)

    # Trigger event pipeline
    publish_message(REQUEST_TOPIC, {"job_id": job_id, "topic": topic, "status": "planning", "retry_count": 0})
    return job


@app.get("/research/{job_id}")
def get_research_job_details(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    logs = get_job_logs(job_id)
    job_data = dict(job)
    job_data["logs"] = logs
    return job_data


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await ws_manager.connect(job_id, websocket)
    try:
        # Send initial status snapshot upon connection
        job = get_job(job_id)
        if job:
            await websocket.send_json({
                "job_id": job_id,
                "stage": job.get("current_stage", "planning"),
                "status": job.get("status", "planning"),
                "retry_count": job.get("retry_count", 0),
                "message": f"Connected to live updates for job {job_id}",
                "report": job.get("report"),
            })

        while True:
            # Keep socket alive and receive client heartbeats/messages
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(job_id, websocket)
    except Exception as e:
        logger.warning(f"WebSocket error for job {job_id}: {e}")
        ws_manager.disconnect(job_id, websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)

