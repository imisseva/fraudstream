"""Optional Kafka integration test.

Run only when KAFKA_INTEGRATION=1 and a dedicated test broker is configured.
The normal CI suite intentionally skips this test.
"""

import os
import time
import uuid

import pytest


@pytest.mark.skipif(os.environ.get("KAFKA_INTEGRATION") != "1", reason="Kafka integration test is opt-in")
def test_kafka_integration_requires_explicit_test_environment():
    required = ["BOOTSTRAP_SERVERS", "SASL_USERNAME", "SASL_PASSWORD", "KAFKA_CA_PATH"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        pytest.fail(f"Missing integration-test environment variables: {', '.join(missing)}")

    from confluent_kafka import Consumer, Producer

    topic = os.environ.get("KAFKA_TEST_TOPIC", "fraudstream-integration-test")
    common = {
        "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
        "security.protocol": os.environ.get("SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.environ.get("SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": os.environ["SASL_USERNAME"],
        "sasl.password": os.environ["SASL_PASSWORD"],
        "ssl.ca.location": os.environ["KAFKA_CA_PATH"],
    }
    key = f"integration-{uuid.uuid4()}"
    producer = Producer({**common, "acks": "all"})
    producer.produce(topic, key=key, value='{"event_type":"integration_test"}')
    producer.flush(15)

    consumer = Consumer({
        **common,
        "group.id": f"fraudstream-integration-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    deadline = time.time() + 20
    try:
        while time.time() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                continue
            if message.key() == key:
                return
        pytest.fail("Produced Kafka integration message was not consumed before timeout")
    finally:
        consumer.close()
