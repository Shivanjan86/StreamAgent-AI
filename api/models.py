from pydantic import BaseModel


class ResearchRequest(BaseModel):
    topic: str


class ResearchJob(BaseModel):
    id: str
    topic: str
    status: str
    report: str | None = None
