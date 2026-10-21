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
    def __init__(
        self,
        bootstrap_servers: str,
        topic: Optional[str] = None,
        topic_transactions: str = "transaction-events",
        topic_clickstream: str = "clickstream-events",
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        dry_run: bool = False,
    ):
        self.default_topic = topic or topic_transactions
        self.topic_transactions = topic_transactions
        self.topic_clickstream = topic_clickstream
        self.dry_run = dry_run
        self.producer = None

        if not self.dry_run:
            if not CONFLUENT_KAFKA_AVAILABLE:
                raise ImportError(
                    "confluent-kafka is not installed. Run 'pip install confluent-kafka' "
                    "or enable dry-run mode."
                )
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 10,           # Gom events 10ms rồi gửi 1 lần
                "batch.num.messages": 100,  # Gom tối đa 100 messages/batch
                "acks": "all",
            }
            # Cấu hình SASL/SSL cho Cloud (Aiven, Confluent Cloud...)
            if sasl_username and sasl_password:
                conf["security.protocol"] = security_protocol or "SASL_SSL"
                conf["sasl.mechanisms"] = sasl_mechanism or "SCRAM-SHA-256"
                conf["sasl.username"] = sasl_username
                conf["sasl.password"] = sasl_password
                # Nếu chạy trên Windows kết nối Cloud không có root CA cục bộ
                conf["enable.ssl.certificate.verification"] = False

            self.producer = Producer(conf)

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")

    def resolve_topic(self, event: Dict[str, Any], explicit_topic: Optional[str] = None) -> str:
        if explicit_topic:
            return explicit_topic
        event_type = event.get("event_type", "")
        if event_type == "transaction":
            return self.topic_transactions
        return self.topic_clickstream

    async def send_event(self, event: Dict[str, Any], topic: Optional[str] = None):
        """Gửi 1 event vào Kafka theo topic tương ứng."""
        target_topic = self.resolve_topic(event, topic)

        if self.dry_run:
            logger.debug(f"[DRY RUN -> {target_topic}] {event.get('event_type')} user={event.get('user_id')}")
            return

        key = event.get("user_id", event.get("card_id", "unknown"))
        value = json.dumps(event, ensure_ascii=False)
        
        self.producer.produce(
            topic=target_topic,
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
