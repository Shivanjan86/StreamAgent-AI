import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "research.db"))


def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            status TEXT NOT NULL,
            current_stage TEXT NOT NULL,
            retry_count INTEGER DEFAULT 0,
            report TEXT,
            error TEXT,
            created_at REAL,
            updated_at REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stage_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            timestamp REAL,
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
    """)
    conn.commit()
    conn.close()


def create_job(job_id: str, topic: str) -> Dict[str, Any]:
    now = time.time()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO jobs (id, topic, status, current_stage, retry_count, created_at, updated_at)
        VALUES (?, ?, 'planning', 'planning', 0, ?, ?)
        """,
        (job_id, topic, now, now),
    )
    conn.commit()
    conn.close()
    return get_job(job_id)


def update_job(
    job_id: str,
    status: Optional[str] = None,
    current_stage: Optional[str] = None,
    retry_count: Optional[int] = None,
    report: Optional[str] = None,
    error: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor()
    now = time.time()

    fields = ["updated_at = ?"]
    params = [now]

    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if current_stage is not None:
        fields.append("current_stage = ?")
        params.append(current_stage)
    if retry_count is not None:
        fields.append("retry_count = ?")
        params.append(retry_count)
    if report is not None:
        fields.append("report = ?")
        params.append(report)
    if error is not None:
        fields.append("error = ?")
        params.append(error)

    params.append(job_id)
    query = f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?"
    cursor.execute(query, params)
    conn.commit()
    conn.close()
    return get_job(job_id)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)


def add_stage_log(job_id: str, stage: str, payload: Dict[str, Any]) -> None:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO stage_logs (job_id, stage, payload_json, timestamp) VALUES (?, ?, ?, ?)",
        (job_id, stage, json.dumps(payload, ensure_ascii=False), time.time()),
    )
    conn.commit()
    conn.close()


def get_job_logs(job_id: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM stage_logs WHERE job_id = ? ORDER BY id ASC", (job_id,))
    rows = cursor.fetchall()
    conn.close()
    logs = []
    for r in rows:
        d = dict(r)
        d["payload"] = json.loads(d["payload_json"])
        logs.append(d)
    return logs

