# StreamAgent AI ⚡

**Real-Time Multi-Agent Deep Research Generator powered by Redpanda Kafka, FastAPI, React, and WebSockets.**

## 🎯 Architecture Overview

StreamAgent AI decomposes deep research into a 5-stage event-driven agent pipeline:

1. **Planner Agent**: Deconstructs research topic into targeted sub-questions and outline (`research.requested` → `research.planned`).
2. **Searcher Agent**: Executes live DuckDuckGo web searches and fetches real source citations (`research.planned` → `research.searched`).
3. **Summarizer Agent**: Condenses web findings into per-section technical notes (`research.searched` → `research.summarized`).
4. **Critic Agent**: Performs fact-checking and triggers Redo Loops if section depth is insufficient (`research.summarized` → `research.critiqued`).
5. **Compiler Agent**: Assembles final markdown report with citations and executive summary (`research.critiqued` → `research.completed`).

Real-time stage transitions are streamed via **Redpanda Cloud Kafka** and broadcasted live to the React UI using a **WebSocket Status Relay**.

---

## 🛠️ Tech Stack

- **Frontend**: React, Vite, CSS Glassmorphism
- **Messaging / Event Bus**: Redpanda Cloud Kafka
- **Backend API**: FastAPI, Uvicorn, Async WebSockets
- **Database**: SQLite / PostgreSQL
- **Language**: Python 3.11+
