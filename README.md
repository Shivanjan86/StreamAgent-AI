# Multi-Agent Research Report Generator

FastAPI + React + LangChain + Kafka demo for a multi-stage research report pipeline.

## Structure

- `frontend/` - React UI
- `api/` - FastAPI service
- `agents/` - Kafka worker processes
- `status_relay/` - WebSocket status relay
- `shared/` - Common config and schemas

## Next steps

1. Wire up the frontend shell.
2. Add FastAPI endpoints.
3. Implement Kafka message schemas.
4. Build the agent workers stage by stage.
