import logging
import time
from typing import Any, Dict, List

try:
    from ...api.db import add_stage_log, update_job
    from ...api.kafka_producer import publish_message
    from ...shared.hf_client import MISTRAL_7B, call_hf_llm
    from ...shared.kafka_config import SEARCHED_TOPIC, SUMMARIZED_TOPIC, event_bus
except Exception:
    from api.db import add_stage_log, update_job
    from api.kafka_producer import publish_message
    from shared.hf_client import MISTRAL_7B, call_hf_llm
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

    section_notes = []

    for sq in sub_questions:
        sq_id = sq["id"]
        focus = sq.get("focus_area", f"Section {sq_id}")
        q_text = sq["question"]
        sq_sources = [s for s in sources if s.get("sub_question_id") == sq_id]

        citations = [s["url"] for s in sq_sources if "url" in s]

        # Try Hugging Face model for synthesis
        raw_snippets = "\n".join([f"- {s.get('title')}: {s.get('snippet')}" for s in sq_sources])
        prompt = (
            f"Research Question: {q_text}\n"
            f"Topic: {topic}\n"
            f"Web Sources & Snippets:\n{raw_snippets}\n\n"
            "Summarize the key insights into 3 concise, highly technical bullet points. Format each bullet point as '• **[Short Title]**: [Detailed Insight]'"
        )

        hf_summary = call_hf_llm(
            prompt,
            system_prompt="You are a senior technical analyst synthesizing research papers and market data.",
            model=MISTRAL_7B,
            max_tokens=600,
        )

        findings = []
        if hf_summary:
            for line in hf_summary.strip().split("\n"):
                if line.strip():
                    findings.append(line.strip())

        if not findings:
            for s in sq_sources:
                findings.append(f"• **{s['title']}**: {s['snippet']}")

        if not findings:
            findings.append(f"• Baseline research indicates rapid developments in {topic} regarding {focus}.")

        section_notes.append({
            "sub_question_id": sq_id,
            "section_title": focus,
            "question": q_text,
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

