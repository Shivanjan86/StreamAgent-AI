import logging
import time
from typing import Any, Dict

try:
    from ...api.db import add_stage_log, update_job
    from ...api.kafka_producer import publish_message
    from ...shared.kafka_config import CRITIQUED_TOPIC, PLANNED_TOPIC, SUMMARIZED_TOPIC, event_bus
except Exception:
    from api.db import add_stage_log, update_job
    from api.kafka_producer import publish_message
    from shared.kafka_config import CRITIQUED_TOPIC, PLANNED_TOPIC, SUMMARIZED_TOPIC, event_bus

logger = logging.getLogger("agent.critic")

MAX_RETRIES = 1  # 1 redo cycle allowed for demonstration


def run_critic_agent(payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = payload["job_id"]
    topic = payload["topic"]
    retry_count = payload.get("retry_count", 0)
    section_notes = payload.get("section_notes", [])
    sources = payload.get("sources", [])

    logger.info(f"[Critic] Fact-checking and reviewing report notes for job {job_id} (retry: {retry_count})")
    update_job(job_id, status="critiquing", current_stage="critiquing")

    time.sleep(0.7)  # simulate fact checking time

    # Decide if redo is needed (demonstrate Redo Loop if topic asks or on initial pass with keyword trigger, but bound by MAX_RETRIES)
    should_redo = False
    feedback = ""

    # If topic has specific keyword 'redo' or if first pass and topic length > 25, simulate a constructive redo suggestion once
    if retry_count < MAX_RETRIES and ("redo" in topic.lower() or "deep" in topic.lower()):
        should_redo = True
        feedback = "Section 3 challenges need deeper technical citations and updated 2026 data metrics."

    if should_redo:
        new_retry_count = retry_count + 1
        logger.warning(f"[Critic] Flagged weak section. Triggering REDO LOOP (retry {new_retry_count}) -> back to Planner!")

        redo_payload = {
            "job_id": job_id,
            "topic": topic,
            "sub_questions": payload.get("sub_questions", []),
            "outline": payload.get("outline", []),
            "retry_count": new_retry_count,
            "feedback": feedback,
        }

        update_job(job_id, status="planned", current_stage="planned", retry_count=new_retry_count)
        add_stage_log(job_id, "critic_redo", redo_payload)

        # Emit BACK to Planner topic
        publish_message(PLANNED_TOPIC, redo_payload)
        return redo_payload

    # Otherwise Approved!
    approved_payload = {
        "job_id": job_id,
        "topic": topic,
        "approved": True,
        "quality_score": 94,
        "feedback": "All sections fact-checked. Technical depth verified with valid source citations.",
        "sub_questions": payload.get("sub_questions", []),
        "outline": payload.get("outline", []),
        "section_notes": section_notes,
        "sources": sources,
        "retry_count": retry_count,
    }

    update_job(job_id, status="critiqued", current_stage="critiqued")
    add_stage_log(job_id, "critic", approved_payload)

    # Publish to next stage: research.critiqued
    publish_message(CRITIQUED_TOPIC, approved_payload)
    return approved_payload


def register_critic_worker():
    event_bus.subscribe(SUMMARIZED_TOPIC, run_critic_agent)
    logger.info("Critic Agent worker registered on topic: %s", SUMMARIZED_TOPIC)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    register_critic_worker()

