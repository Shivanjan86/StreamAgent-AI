import asyncio
import json
import logging
import os
import threading
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

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or None
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME") or None
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD") or None


class EventBus:
    """In-memory Async Event Bus + Redpanda Kafka Consumer Router."""

    _listeners: Dict[str, List[Callable[[Dict[str, Any]], Any]]] = {}
    _subscribed_kafka_topics: set = set()

    @classmethod
    def subscribe(cls, topic: str, handler: Callable[[Dict[str, Any]], Any]) -> None:
        if topic not in cls._listeners:
            cls._listeners[topic] = []
        cls._listeners[topic].append(handler)
        logger.info(f"Subscribed handler to topic: {topic}")

        # If real Redpanda / Kafka credentials exist, spin up a consumer thread for this topic
        if BOOTSTRAP and topic not in cls._subscribed_kafka_topics:
            cls._subscribed_kafka_topics.add(topic)
            threading.Thread(target=cls._kafka_consumer_loop, args=(topic,), daemon=True).start()

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

    @classmethod
    def _kafka_consumer_loop(cls, topic: str) -> None:
        """Background Kafka Consumer loop for Redpanda Cloud integration."""
        try:
            from confluent_kafka import Consumer

            conf = {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": f"worker-group-{topic}",
                "auto.offset.reset": "latest",
            }
            if SASL_USERNAME and SASL_PASSWORD:
                conf.update({
                    "security.protocol": SECURITY_PROTOCOL,
                    "sasl.mechanisms": SASL_MECHANISM,
                    "sasl.username": SASL_USERNAME,
                    "sasl.password": SASL_PASSWORD,
                })
            consumer = Consumer(conf)
            consumer.subscribe([topic])
            logger.info(f"[Redpanda Consumer] Listening to remote Kafka topic: {topic}")

            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning(f"[Redpanda Consumer] Error on topic {topic}: {msg.error()}")
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    cls.publish(topic, payload)
                except Exception as ex:
                    logger.error(f"[Redpanda Consumer] Failed to process message: {ex}")
        except Exception as e:
            logger.info(f"[Redpanda Consumer] Could not start consumer loop for '{topic}': {e}")


event_bus = EventBus()



