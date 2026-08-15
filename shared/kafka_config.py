import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

REQUEST_TOPIC = "research.requested"
PLANNED_TOPIC = "research.planned"
SEARCHED_TOPIC = "research.searched"
SUMMARIZED_TOPIC = "research.summarized"
CRITIQUED_TOPIC = "research.critiqued"
COMPLETED_TOPIC = "research.completed"
DLQ_TOPIC = "research.dlq"

logger = logging.getLogger("shared.event_bus")
_executor = ThreadPoolExecutor(max_workers=16)


class EventBus:
    """In-memory Async Event Bus with Redpanda Cloud Integration."""

    _listeners: Dict[str, List[Callable[[Dict[str, Any]], Any]]] = {}

    @classmethod
    def subscribe(cls, topic: str, handler: Callable[[Dict[str, Any]], Any]) -> None:
        if topic not in cls._listeners:
            cls._listeners[topic] = []
        cls._listeners[topic].append(handler)
        logger.info(f"Subscribed handler to topic: {topic}")

    @classmethod
    def publish(cls, topic: str, payload: Dict[str, Any]) -> None:
        logger.info(f"[EventBus] Dispatching message on topic '{topic}'")
        handlers = cls._listeners.get(topic, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(handler(payload))
                    except RuntimeError:
                        asyncio.run(handler(payload))
                else:
                    _executor.submit(handler, payload)
            except Exception as e:
                logger.error(f"[EventBus] Error dispatching to handler on topic '{topic}': {e}", exc_info=True)


event_bus = EventBus()
