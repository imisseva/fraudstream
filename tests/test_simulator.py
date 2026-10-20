"""Unit tests for FraudStream Simulator."""

import json
from pathlib import Path
import pytest

from simulator.config import SimulatorConfig, KafkaConfig, HOURLY_TRAFFIC
from simulator.models.user import User, UserType
from simulator.models.product import ProductCatalog, Product
from simulator.models.session import Session, SessionStage
from simulator.generators.event_generator import EventGenerator
from simulator.generators.fraud_generator import FraudGenerator
from simulator.generators.traffic_pattern import TrafficPattern
from simulator.producers.kafka_producer import KafkaEventProducer


@pytest.fixture
def data_dir():
    return Path(__file__).parent.parent / "simulator" / "data"


@pytest.fixture
def catalog(data_dir):
    return ProductCatalog(data_dir / "products.json")


@pytest.fixture
def cities(data_dir):
    with open(data_dir / "cities.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def devices(data_dir):
    with open(data_dir / "devices.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def config():
    return SimulatorConfig(
        total_users=100,
        base_events_per_second=20.0,
        dry_run=True,
    )


# -------------------------------------------------------------
# Test 1: Data Files & Catalog
# -------------------------------------------------------------

def test_data_files_exist_and_valid(data_dir, catalog, cities, devices):
    assert len(catalog.products) >= 50
    assert len(catalog.high_risk) > 0
    assert len(cities) >= 15
    assert len(devices) >= 10

    # Ensure all products have required attributes
    for p in catalog.products:
        assert p.product_id
        assert p.name
        assert p.category
        assert p.price > 0
        assert isinstance(p.is_high_risk, bool)


def test_catalog_category_filtering(catalog):
    electronics = catalog.get_random_product("electronics")
    assert electronics.category == "electronics"

    high_risk = catalog.get_high_risk_product()
    assert high_risk.is_high_risk is True


# -------------------------------------------------------------
# Test 2: User Creation & Types
# -------------------------------------------------------------

def test_user_creation(cities, devices):
    normal = User.create_normal(cities, devices)
    assert normal.user_type == UserType.NORMAL
    assert normal.user_id.startswith("user_")
    assert len(normal.preferred_categories) == 3

    suspicious = User.create_suspicious(cities, devices)
    assert suspicious.user_type == UserType.SUSPICIOUS
    assert "gift_cards" in suspicious.preferred_categories

    fraudster = User.create_fraudster(cities, devices)
    assert fraudster.user_type == UserType.FRAUDSTER
    assert fraudster.user_id.startswith("fraud_")
    assert fraudster.max_orders_per_day >= 50


# -------------------------------------------------------------
# Test 3: Session State
# -------------------------------------------------------------

def test_session_lifecycle():
    sess = Session(ip_address="103.12.44.10", city="Ho Chi Minh")
    assert sess.is_active() is True
    assert sess.stage == SessionStage.LANDING

    sess.stage = SessionStage.COMPLETED
    assert sess.is_active() is False

    sess.stage = SessionStage.ABANDONED
    assert sess.is_active() is False


# -------------------------------------------------------------
# Test 4: Event Generator Funnel & Fields
# -------------------------------------------------------------

def test_event_has_required_fields(config, catalog, cities, devices):
    gen = EventGenerator(config, catalog)
    user = User.create_normal(cities, devices)
    session = Session(ip_address="103.12.44.1", city="Ho Chi Minh")

    event = gen.generate_next_event(user, session)
    assert event is not None

    required_keys = [
        "event_id",
        "timestamp",
        "user_id",
        "session_id",
        "ip_address",
        "device",
        "city",
        "user_type",
        "event_type",
    ]
    for key in required_keys:
        assert key in event, f"Missing required key: {key}"

    assert event["event_type"] == "page_view"
    assert session.stage == SessionStage.BROWSING


def test_normal_user_full_funnel(config, catalog, cities, devices):
    # Set 100% conversion to verify full funnel progression
    custom_config = SimulatorConfig(
        search_after_pageview=1.0,
        add_to_cart_rate=1.0,
        purchase_rate=1.0,
        dry_run=True,
    )
    gen = EventGenerator(custom_config, catalog)
    user = User.create_normal(cities, devices)
    session = Session(ip_address="103.12.44.1", city="Ho Chi Minh")

    # Step 1: Landing -> page_view
    e1 = gen.generate_next_event(user, session)
    assert e1["event_type"] == "page_view"
    assert session.stage == SessionStage.BROWSING

    # Step 2: Browsing -> search
    e2 = gen.generate_next_event(user, session)
    assert e2["event_type"] == "search"
    assert "query" in e2

    # Step 3: Product View
    e3 = gen._gen_product_view(
        {
            "event_id": "test",
            "timestamp": "2026-10-21T00:00:00Z",
            "user_id": user.user_id,
            "session_id": session.session_id,
            "ip_address": session.ip_address,
            "device": session.device,
            "city": session.city,
            "user_type": user.user_type.value,
        },
        user,
        session,
    )
    assert e3["event_type"] == "product_view"
    assert session.stage == SessionStage.PRODUCT_VIEWING

    # Step 4: Add to cart
    e4 = gen.generate_next_event(user, session)
    assert e4["event_type"] == "add_to_cart"
    assert session.stage == SessionStage.CARTING

    # Step 5: Purchase
    e5 = gen.generate_next_event(user, session)
    assert e5["event_type"] == "purchase"
    assert session.stage == SessionStage.COMPLETED
    assert "order_id" in e5
    assert len(e5["items"]) > 0


# -------------------------------------------------------------
# Test 5: Fraud Generator Scenarios
# -------------------------------------------------------------

def test_fraud_scenarios(catalog, cities, devices):
    fraud_gen = FraudGenerator(catalog, cities)
    fraudster = User.create_fraudster(cities, devices)
    session = Session(ip_address="103.12.44.1", city="Ho Chi Minh")

    # Test card testing
    ct_event = fraud_gen._card_testing(fraudster, session)
    assert ct_event["event_type"] == "purchase"
    assert ct_event["fraud_scenario"] == "card_testing"
    assert ct_event["user_type"] == "fraudster"

    # Test account takeover
    at_event = fraud_gen._account_takeover(fraudster, session)
    assert at_event["event_type"] == "purchase"
    assert at_event["fraud_scenario"] == "account_takeover"
    assert "fp_" in at_event["device"]["fingerprint"]

    # Test bot attack
    bot_event = fraud_gen._bot_attack(fraudster, session)
    assert bot_event["event_type"] == "product_view"
    assert bot_event["fraud_scenario"] == "bot_attack"


# -------------------------------------------------------------
# Test 6: Traffic Pattern
# -------------------------------------------------------------

def test_traffic_pattern():
    tp = TrafficPattern(base_rate=20.0)
    rate = tp.get_current_rate()
    assert 0 < rate <= 20.0

    interval = tp.get_sleep_interval()
    assert interval > 0

    assert len(HOURLY_TRAFFIC) == 24


# -------------------------------------------------------------
# Test 7: Kafka Producer Dry Run
# -------------------------------------------------------------

@pytest.mark.asyncio
async def test_kafka_producer_dry_run():
    producer = KafkaEventProducer(
        bootstrap_servers="localhost:9092",
        topic="test-topic",
        dry_run=True,
    )
    test_event = {"event_id": "test_1", "event_type": "page_view", "user_id": "u1"}
    await producer.send_event(test_event)
    producer.flush()
    producer.close()

