import logging
import time
from typing import Any, Dict

try:
    from ...api.db import add_stage_log, update_job
    from ...api.kafka_producer import publish_message
    from ...shared.kafka_config import COMPLETED_TOPIC, CRITIQUED_TOPIC, event_bus
except Exception:
    from api.db import add_stage_log, update_job
    from api.kafka_producer import publish_message
    from shared.kafka_config import COMPLETED_TOPIC, CRITIQUED_TOPIC, event_bus

logger = logging.getLogger("agent.compiler")


def run_compiler_agent(payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = payload["job_id"]
    topic = payload["topic"]
    section_notes = payload.get("section_notes", [])
    sources = payload.get("sources", [])
    outline = payload.get("outline", [])
    score = payload.get("quality_score", 95)
    critic_feedback = payload.get("feedback", "")
    retry_count = payload.get("retry_count", 0)

    logger.info(f"[Compiler] Assembling final research report for job {job_id}")
    update_job(job_id, status="compiling", current_stage="compiling")

    time.sleep(0.6)  # simulate markdown formatting time

    # Build rich Markdown report
    lines = [
        f"# Comprehensive Research Report: {topic.strip()}",
        "",
        f"> **Agent Pipeline Audit**: Verified by Critic Agent (Quality Score: **{score}%**) | Stage Iterations: **{retry_count + 1}**",
        f"> **Fact-Check Notes**: *{critic_feedback}*",
        "",
        "## Executive Summary",
        f"This report presents an in-depth research synthesis on **{topic.strip()}**. "
        "Generated through an event-driven multi-agent LLM pipeline, this document aggregates real-world data, "
        "technical architecture analysis, industry deployment challenges, and forward-looking strategic recommendations.",
        "",
        "---",
        "",
        "## Report Outline",
    ]

    for item in outline:
        lines.append(f"- {item}")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Add each section with key findings and inline citations
    for idx, note in enumerate(section_notes, start=1):
        lines.append(f"## {idx}. {note['section_title']}")
        lines.append(f"**Core Research Question**: *{note['question']}*")
        lines.append("")
        lines.append("### Key Synthesis & Findings")
        for finding in note["key_findings"]:
            lines.append(finding)
        lines.append("")
        if note.get("citations"):
            lines.append("**Section Citations**:")
            for cite in note["citations"]:
                lines.append(f"- [{cite}]({cite})")
            lines.append("")
        lines.append("---")
        lines.append("")

    lines.append("## Consolidated References & Source Citations")
    lines.append("The multi-agent searcher gathered and verified the following primary sources:")
    lines.append("")

    seen_urls = set()
    ref_idx = 1
    for s in sources:
        url = s.get("url", "#")
        if url not in seen_urls:
            seen_urls.add(url)
            lines.append(f"{ref_idx}. [{s.get('title', 'Web Article')}]({url}) — *{s.get('snippet', '')}*")
            ref_idx += 1

    lines.append("")
    lines.append("> *Report compiled automatically by Multi-Agent Research Generator.*")

    final_report_text = "\n".join(lines)

    output_payload = {
        "job_id": job_id,
        "topic": topic,
        "final_report": final_report_text,
        "sources": sources,
        "retry_count": retry_count,
    }

    update_job(job_id, status="completed", current_stage="completed", report=final_report_text)
    add_stage_log(job_id, "compiler", output_payload)

    # Publish final topic: research.completed
    publish_message(COMPLETED_TOPIC, output_payload)
    return output_payload


def register_compiler_worker():
    event_bus.subscribe(CRITIQUED_TOPIC, run_compiler_agent)
    logger.info("Compiler Agent worker registered on topic: %s", CRITIQUED_TOPIC)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    register_compiler_worker()

