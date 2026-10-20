import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

try:
    from confluent_kafka import Producer
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    Producer = None
    CONFLUENT_KAFKA_AVAILABLE = False


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str, dry_run: bool = False):
        self.topic = topic
        self.dry_run = dry_run
        self.producer = None

        if not self.dry_run:
            if not CONFLUENT_KAFKA_AVAILABLE:
                raise ImportError(
                    "confluent-kafka is not installed. Run 'pip install confluent-kafka' "
                    "or enable dry-run mode."
                )
            self.producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 10,           # Gom events 10ms rồi gửi 1 lần
                "batch.num.messages": 100,  # Gom tối đa 100 messages/batch
                "acks": "all",
            })

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")

    async def send_event(self, event: Dict[str, Any]):
        """Gửi 1 event vào Kafka"""
        if self.dry_run:
            logger.debug(f"[DRY RUN] {event.get('event_type')} user={event.get('user_id')}")
            return

        key = event.get("user_id", "unknown")
        value = json.dumps(event, ensure_ascii=False)
        
        self.producer.produce(
            topic=self.topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=self._delivery_callback,
        )
        self.producer.poll(0)  # Non-blocking poll

    def flush(self):
        """Flush tất cả messages còn trong buffer"""
        if self.producer is not None:
            self.producer.flush(timeout=10)

    def close(self):
        self.flush()

