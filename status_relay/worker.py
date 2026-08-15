import asyncio
import logging
from typing import Any, Dict, List
from fastapi import WebSocket

try:
    from ..shared.kafka_config import (
        COMPLETED_TOPIC,
        CRITIQUED_TOPIC,
        PLANNED_TOPIC,
        REQUEST_TOPIC,
        SEARCHED_TOPIC,
        SUMMARIZED_TOPIC,
        event_bus,
    )
except Exception:
    from shared.kafka_config import (
        COMPLETED_TOPIC,
        CRITIQUED_TOPIC,
        PLANNED_TOPIC,
        REQUEST_TOPIC,
        SEARCHED_TOPIC,
        SUMMARIZED_TOPIC,
        event_bus,
    )

logger = logging.getLogger("status_relay")


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, job_id: str, websocket: WebSocket):
        await websocket.accept()
        if job_id not in self.active_connections:
            self.active_connections[job_id] = []
        self.active_connections[job_id].append(websocket)
        logger.info(f"[WebSocket] Client connected for job {job_id}")

    def disconnect(self, job_id: str, websocket: WebSocket):
        if job_id in self.active_connections:
            if websocket in self.active_connections[job_id]:
                self.active_connections[job_id].remove(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]
        logger.info(f"[WebSocket] Client disconnected for job {job_id}")

    async def broadcast_to_job(self, job_id: str, message: dict):
        if job_id in self.active_connections:
            dead_sockets = []
            for connection in self.active_connections[job_id]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.warning(f"[WebSocket] Send failed: {e}")
                    dead_sockets.append(connection)
            for dead in dead_sockets:
                self.disconnect(job_id, dead)


manager = ConnectionManager()


async def handle_stage_event(topic: str, payload: Dict[str, Any]):
    job_id = payload.get("job_id")
    if not job_id:
        return

    stage_name = topic.replace("research.", "")
    retry_count = payload.get("retry_count", 0)

    ws_message = {
        "job_id": job_id,
        "topic": topic,
        "stage": stage_name,
        "status": stage_name,
        "retry_count": retry_count,
        "message": f"Stage update: {stage_name}",
        "payload": payload,
    }

    # Broadcast over WebSocket asynchronously
    await manager.broadcast_to_job(job_id, ws_message)


def register_status_relay():
    all_topics = [
        REQUEST_TOPIC,
        PLANNED_TOPIC,
        SEARCHED_TOPIC,
        SUMMARIZED_TOPIC,
        CRITIQUED_TOPIC,
        COMPLETED_TOPIC,
    ]
    for topic in all_topics:

        def make_handler(t=topic):
            def handler(payload):
                try:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(handle_stage_event(t, payload))
                    except RuntimeError:
                        new_loop = asyncio.new_event_loop()
                        new_loop.run_until_complete(handle_stage_event(t, payload))
                        new_loop.close()
                except Exception as e:
                    logger.warning(f"Error handling status relay for topic {t}: {e}")

            return handler

        event_bus.subscribe(topic, make_handler(topic))

    logger.info("Status Relay registered across all stage topics.")



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    register_status_relay()

