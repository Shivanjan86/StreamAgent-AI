from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import time


class ResearchJob(BaseModel):
    id: str
    topic: str
    status: str = "planning"  # planning, planned, searching, searched, summarizing, summarized, critiquing, critiqued, completed, failed
    current_stage: str = "planning"
    retry_count: int = 0
    report: Optional[str] = None
    error: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)


class StageMessage(BaseModel):
    job_id: str
    stage: str
    topic: str
    payload: Dict[str, Any]
    retry_count: int = 0
    timestamp: float = Field(default_factory=time.time)


class SubQuestion(BaseModel):
    id: int
    question: str
    focus_area: str


class PlannerPayload(BaseModel):
    topic: str
    sub_questions: List[SubQuestion]
    outline: List[str]


class SourceItem(BaseModel):
    url: str
    title: str
    snippet: str
    sub_question_id: int


class SearcherPayload(BaseModel):
    topic: str
    sources: List[SourceItem]


class SectionNote(BaseModel):
    sub_question_id: int
    section_title: str
    key_findings: List[str]
    citations: List[str]


class SummarizerPayload(BaseModel):
    topic: str
    section_notes: List[SectionNote]


class CriticPayload(BaseModel):
    topic: str
    approved: bool
    quality_score: int
    feedback: str
    section_notes: List[SectionNote]
    retry_count: int = 0


class CompilerPayload(BaseModel):
    topic: str
    final_report: str
    sources: List[SourceItem]


class WebSocketUpdate(BaseModel):
    job_id: str
    stage: str
    status: str
    message: str
    payload: Optional[Dict[str, Any]] = None
    retry_count: int = 0
    timestamp: float = Field(default_factory=time.time)

