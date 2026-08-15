import logging
import time
from typing import Any, Dict, List

try:
    from ...api.db import add_stage_log, update_job
    from ...api.kafka_producer import publish_message
    from ...shared.kafka_config import SEARCHED_TOPIC, SUMMARIZED_TOPIC, event_bus
except Exception:
    from api.db import add_stage_log, update_job
    from api.kafka_producer import publish_message
    from shared.kafka_config import SEARCHED_TOPIC, SUMMARIZED_TOPIC, event_bus

logger = logging.getLogger("agent.summarizer")


def run_summarizer_agent(payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = payload["job_id"]
    topic = payload["topic"]
    sub_questions = payload.get("sub_questions", [])
    sources = payload.get("sources", [])
    retry_count = payload.get("retry_count", 0)

    logger.info(f"[Summarizer] Synthesizing notes for job {job_id} across {len(sources)} sources")
    update_job(job_id, status="summarizing", current_stage="summarizing")

    time.sleep(0.7)  # simulate synthesis processing time

    section_notes = []

    for sq in sub_questions:
        sq_id = sq["id"]
        focus = sq.get("focus_area", f"Section {sq_id}")
        sq_sources = [s for s in sources if s.get("sub_question_id") == sq_id]

        citations = [s["url"] for s in sq_sources if "url" in s]

        findings = []
        for s in sq_sources:
            findings.append(f"• **{s['title']}**: {s['snippet']}")

        if not findings:
            findings.append(f"• Baseline research indicates rapid developments in {topic} regarding {focus}.")

        section_notes.append({
            "sub_question_id": sq_id,
            "section_title": focus,
            "question": sq["question"],
            "key_findings": findings,
            "citations": citations,
        })

    output_payload = {
        "job_id": job_id,
        "topic": topic,
        "sub_questions": sub_questions,
        "outline": payload.get("outline", []),
        "section_notes": section_notes,
        "sources": sources,
        "retry_count": retry_count,
    }

    update_job(job_id, status="summarized", current_stage="summarized")
    add_stage_log(job_id, "summarizer", output_payload)

    # Publish to next stage: research.summarized
    publish_message(SUMMARIZED_TOPIC, output_payload)
    return output_payload


def register_summarizer_worker():
    event_bus.subscribe(SEARCHED_TOPIC, run_summarizer_agent)
    logger.info("Summarizer Agent worker registered on topic: %s", SEARCHED_TOPIC)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    register_summarizer_worker()

