import json
import logging
import os
import sys
import time
import uuid

# Ensure project root directory is in sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    from shared.kafka_config import event_bus
except Exception:
    from ..shared.kafka_config import event_bus

LOG = logging.getLogger("api.kafka_producer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or None
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME") or None
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD") or None

_producer = None
_producer_type = None

# Attempt confluent-kafka initialization
if BOOTSTRAP:
    try:
        from confluent_kafka import Producer

        conf = {"bootstrap.servers": BOOTSTRAP}
        if SASL_USERNAME and SASL_PASSWORD:
            conf.update({
                "security.protocol": SECURITY_PROTOCOL,
                "sasl.mechanisms": SASL_MECHANISM,
                "sasl.username": SASL_USERNAME,
                "sasl.password": SASL_PASSWORD,
            })
        _producer = Producer(conf)
        _producer_type = "confluent"
        LOG.info("Configured confluent-kafka producer for %s", BOOTSTRAP)
    except Exception as e:
        LOG.debug("confluent_kafka producer setup failed: %s", e)

# Fallback to kafka-python if confluent-kafka is not available
if _producer is None and BOOTSTRAP:
    try:
        from kafka import KafkaProducer

        kw = {
            "bootstrap_servers": BOOTSTRAP,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        }
        if SASL_USERNAME and SASL_PASSWORD:
            kw.update({
                "security_protocol": SECURITY_PROTOCOL,
                "sasl_mechanism": SASL_MECHANISM,
                "sasl_plain_username": SASL_USERNAME,
                "sasl_plain_password": SASL_PASSWORD,
            })
        _producer = KafkaProducer(**kw)
        _producer_type = "kafka_python"
        LOG.info("Configured kafka-python producer for %s", BOOTSTRAP)
    except Exception as e:
        LOG.debug("kafka-python producer setup failed: %s", e)


def _local_write(topic: str, payload: dict) -> None:
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "shared", "local_kafka"))
    os.makedirs(base, exist_ok=True)
    filename = os.path.join(base, f"{topic}.ndjson")
    record = {
        "id": str(uuid.uuid4()),
        "ts": int(time.time()),
        "topic": topic,
        "payload": payload,
    }
    with open(filename, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def publish_message(topic: str, payload: dict) -> None:
    """Publish a JSON payload to the EventBus and Kafka (if enabled)."""
    LOG.info("Publishing message to topic '%s' (job_id: %s)", topic, payload.get("job_id"))

    # Publish to EventBus for active in-memory listeners
    try:
        event_bus.publish(topic, payload)
    except Exception as e:
        LOG.error("Failed to publish to EventBus: %s", e)

    # If real Kafka producer is configured, send to Kafka too
    if _producer is not None:
        try:
            if _producer_type == "confluent":
                _producer.produce(topic, json.dumps(payload).encode("utf-8"))
                _producer.flush(0.1)
            elif _producer_type == "kafka_python":
                _producer.send(topic, payload)
                _producer.flush()
        except Exception:
            LOG.exception("Failed to publish to Kafka broker")

    # Local fallback file logging for inspection
    try:
        _local_write(topic, payload)
    except Exception:
        pass


