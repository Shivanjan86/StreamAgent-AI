import logging
import time
from typing import Any, Dict

try:
    from ...api.db import add_stage_log, update_job
    from ...api.kafka_producer import publish_message
    from ...shared.kafka_config import PLANNED_TOPIC, REQUEST_TOPIC, event_bus
except Exception:
    from api.db import add_stage_log, update_job
    from api.kafka_producer import publish_message
    from shared.kafka_config import PLANNED_TOPIC, REQUEST_TOPIC, event_bus

logger = logging.getLogger("agent.planner")


def run_planner_agent(payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = payload["job_id"]
    topic = payload["topic"]
    retry_count = payload.get("retry_count", 0)
    critique_feedback = payload.get("feedback", None)

    logger.info(f"[Planner] Processing job {job_id} for topic: '{topic}' (retry: {retry_count})")
    update_job(job_id, status="planning", current_stage="planning", retry_count=retry_count)

    time.sleep(0.8)  # simulate agent analysis time

    # Generate structured sub-questions tailored to topic
    clean_topic = topic.strip()
    sub_questions = [
        {
            "id": 1,
            "question": f"What are the core fundamentals, history, and key definitions of {clean_topic}?",
            "focus_area": "Fundamentals & Background",
        },
        {
            "id": 2,
            "question": f"What are the primary current applications, industry implementations, and case studies of {clean_topic}?",
            "focus_area": "Applications & Practical Usage",
        },
        {
            "id": 3,
            "question": f"What are the key technical challenges, limitations, and security/ethical risks associated with {clean_topic}?",
            "focus_area": "Challenges & Risks",
        },
        {
            "id": 4,
            "question": f"What are the future outlooks, upcoming breakthroughs, and market growth projections for {clean_topic} over the next 5 years?",
            "focus_area": "Future Outlook & Market",
        },
    ]

    outline = [
        "1. Executive Summary & Core Definitions",
        "2. Background, Evolution & Technology Architecture",
        "3. Practical Applications & Real-World Use Cases",
        "4. Critical Bottlenecks, Risks & Mitigation Strategies",
        "5. Future Roadmap, Emerging Innovations & Market Outlook",
        "6. Summary Conclusion & References",
    ]

    output_payload = {
        "job_id": job_id,
        "topic": topic,
        "sub_questions": sub_questions,
        "outline": outline,
        "retry_count": retry_count,
        "critique_feedback": critique_feedback,
    }

    if critique_feedback:
        logger.info(f"[Planner] Incorporating Critic Feedback for Redo: {critique_feedback}")

    update_job(job_id, status="planned", current_stage="planned")
    add_stage_log(job_id, "planner", output_payload)

    # Publish to next stage: research.planned
    publish_message(PLANNED_TOPIC, output_payload)
    return output_payload


def register_planner_worker():
    event_bus.subscribe(REQUEST_TOPIC, run_planner_agent)
    logger.info("Planner Agent worker registered on topic: %s", REQUEST_TOPIC)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    register_planner_worker()

