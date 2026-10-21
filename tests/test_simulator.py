"""Comprehensive Test Suite for FraudStream Multi-Scenario Simulator."""

import json
from pathlib import Path
import pytest

from simulator.config import SimulatorConfig, KafkaConfig
from simulator.models.user import User, UserType, Card
from simulator.models.merchant import Merchant, MerchantCatalog
from simulator.models.product import ProductCatalog
from simulator.models.session import Session, SessionStage
from simulator.models.transaction import TransactionEvent
from simulator.models.clickstream import ClickstreamEvent

from simulator.generators.user_generator import UserGenerator
from simulator.generators.transaction_generator import TransactionGenerator
from simulator.generators.clickstream_generator import ClickstreamGenerator
from simulator.generators.data_quality_generator import DataQualityGenerator
from simulator.generators.traffic_pattern import TrafficPattern

from simulator.scenarios.normal import NormalScenario
from simulator.scenarios.high_value import HighValueScenario
from simulator.scenarios.velocity import VelocityScenario
from simulator.scenarios.geographic_anomaly import GeographicAnomalyScenario
from simulator.scenarios.device_ip_sharing import DeviceIpSharingScenario
from simulator.scenarios.repeated_attempts import RepeatedAttemptsScenario

from simulator.producers.kafka_producer import KafkaEventProducer


@pytest.fixture
def data_dir():
    return Path(__file__).parent.parent / "simulator" / "data"


@pytest.fixture
def catalog(data_dir):
    return ProductCatalog(data_dir / "products.json")


@pytest.fixture
def merchant_catalog(data_dir):
    return MerchantCatalog(data_dir / "merchants.json")


@pytest.fixture
def countries(data_dir):
    with open(data_dir / "countries.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def cities(data_dir):
    with open(data_dir / "cities.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def devices(data_dir):
    with open(data_dir / "devices.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def sample_user(countries, cities, devices):
    return User.create_normal(countries, cities, devices)


# -------------------------------------------------------------------
# Test 1: Reference Data Catalogs
# -------------------------------------------------------------------

def test_reference_data_catalogs(merchant_catalog, countries, cities, devices, catalog):
    assert len(merchant_catalog.merchants) >= 20
    assert len(countries) >= 10
    assert len(cities) >= 15
    assert len(devices) >= 10
    assert len(catalog.products) >= 50

    # Ensure devices have device_id
    for dev in devices:
        assert "device_id" in dev


# -------------------------------------------------------------------
# Test 2: User and Card Models
# -------------------------------------------------------------------

def test_user_creation_with_cards(countries, cities, devices):
    user = User.create_normal(countries, cities, devices)
    assert user.user_type == UserType.NORMAL
    assert len(user.cards) >= 1
    assert user.primary_card.card_id
    assert user.device_id
    assert user.avg_transaction_amount > 0
    assert user.max_usual_amount >= user.avg_transaction_amount

    fraudster = User.create_fraudster(countries, cities, devices)
    assert fraudster.user_type == UserType.FRAUDSTER
    assert len(fraudster.cards) >= 2


# -------------------------------------------------------------------
# Test 3: Transaction Schema 16 Fields
# -------------------------------------------------------------------

def test_transaction_event_schema(merchant_catalog, sample_user):
    normal = NormalScenario(merchant_catalog)
    txn = normal.generate(sample_user)
    txn_dict = txn.to_dict()

    expected_fields = [
        "event_id",
        "event_type",
        "event_time",
        "transaction_id",
        "user_id",
        "card_id",
        "merchant_id",
        "amount",
        "currency",
        "country",
        "channel",
        "payment_method",
        "device_id",
        "ip_address",
        "status",
        "is_fraud",
    ]
    for field in expected_fields:
        assert field in txn_dict, f"Missing field: {field}"

    assert txn_dict["event_type"] == "transaction"
    assert txn_dict["is_fraud"] == 0
    assert txn_dict["status"] == "success"


# -------------------------------------------------------------------
# Test 4: Scenario A — Unusually Large Transaction
# -------------------------------------------------------------------

def test_scenario_a_high_value(merchant_catalog, sample_user):
    scenario = HighValueScenario(merchant_catalog)
    txn = scenario.generate(sample_user)

    assert txn.is_fraud == 1
    assert txn.fraud_scenario == "high_value"
    assert txn.amount > sample_user.max_usual_amount
    assert txn.card_id == sample_user.primary_card.card_id


# -------------------------------------------------------------------
# Test 5: Scenario B — Transaction Velocity (Burst)
# -------------------------------------------------------------------

def test_scenario_b_velocity(merchant_catalog, sample_user):
    scenario = VelocityScenario(merchant_catalog)
    burst = scenario.generate_burst(sample_user, burst_count=10)

    assert len(burst) == 10
    card_id = sample_user.primary_card.card_id
    for txn in burst:
        assert txn.card_id == card_id
        assert txn.is_fraud == 1
        assert txn.fraud_scenario == "velocity"


# -------------------------------------------------------------------
# Test 6: Scenario C — Geographic Anomaly (Impossible Travel)
# -------------------------------------------------------------------

def test_scenario_c_geographic_anomaly(merchant_catalog, countries, sample_user):
    scenario = GeographicAnomalyScenario(merchant_catalog, countries)
    txn = scenario.generate(sample_user)

    assert txn.is_fraud == 1
    assert txn.fraud_scenario == "geographic_anomaly"
    assert txn.country != sample_user.home_country


# -------------------------------------------------------------------
# Test 7: Scenario D — Device & IP Sharing (Fraud Ring)
# -------------------------------------------------------------------

def test_scenario_d_device_ip_sharing(merchant_catalog, countries, cities, devices):
    scenario = DeviceIpSharingScenario(merchant_catalog)
    user1 = User.create_normal(countries, cities, devices)
    user2 = User.create_normal(countries, cities, devices)

    txn1 = scenario.generate(user1)
    txn2 = scenario.generate(user2)

    assert txn1.is_fraud == 1
    assert txn2.is_fraud == 1
    assert txn1.device_id == txn2.device_id
    assert txn1.ip_address == txn2.ip_address
    assert txn1.user_id != txn2.user_id


# -------------------------------------------------------------------
# Test 8: Scenario E — Repeated Failed Attempts -> Success
# -------------------------------------------------------------------

def test_scenario_e_repeated_attempts(merchant_catalog, sample_user):
    scenario = RepeatedAttemptsScenario(merchant_catalog)
    seq = scenario.generate_sequence(sample_user, failure_count=3)

    assert len(seq) == 4
    # First 3 should be failed
    for txn in seq[:3]:
        assert txn.status == "failed"
        assert txn.is_fraud == 1
        assert txn.fraud_scenario == "repeated_attempts"
    # Last one should be success
    assert seq[-1].status == "success"
    assert seq[-1].is_fraud == 1


# -------------------------------------------------------------------
# Test 9: Data Quality Scenarios (Glitches)
# -------------------------------------------------------------------

def test_data_quality_glitches():
    dq = DataQualityGenerator(error_rate=1.0)
    sample_dict = {
        "event_id": "evt_test",
        "event_type": "transaction",
        "card_id": "card_test",
        "amount": 100000.0,
        "user_id": "user_test",
        "merchant_id": "mer_test",
        "event_time": "2026-10-21T00:00:00Z",
    }
    dq.record_event(sample_dict)

    # In 10 glitch generations, verify we encounter glitches
    glitches_seen = set()
    for _ in range(30):
        corrupted = dq.inject_glitch(sample_dict)
        if "_data_quality_glitch" in corrupted:
            glitches_seen.add(corrupted["_data_quality_glitch"])

    assert len(glitches_seen) >= 2


# -------------------------------------------------------------------
# Test 10: Clickstream Funnel
# -------------------------------------------------------------------

def test_clickstream_funnel(catalog, sample_user):
    config = SimulatorConfig(search_after_pageview=1.0, add_to_cart_rate=1.0, dry_run=True)
    click_gen = ClickstreamGenerator(config, catalog)
    session = Session(ip_address="103.12.44.1", city="Ho Chi Minh")

    # Step 1: Landing -> page_view
    e1 = click_gen.generate_next_event(sample_user, session)
    assert e1.event_type == "page_view"

    # Step 2: Browsing -> search
    e2 = click_gen.generate_next_event(sample_user, session)
    assert e2.event_type == "search"


# -------------------------------------------------------------------
# Test 11: Kafka Producer Topic Routing
# -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_kafka_producer_topic_routing():
    producer = KafkaEventProducer(
        bootstrap_servers="localhost:9092",
        topic_transactions="custom-txn",
        topic_clickstream="custom-click",
        dry_run=True,
    )
    txn_event = {"event_type": "transaction", "user_id": "u1"}
    click_event = {"event_type": "page_view", "user_id": "u1"}

    assert producer.resolve_topic(txn_event) == "custom-txn"
    assert producer.resolve_topic(click_event) == "custom-click"

    await producer.send_event(txn_event)
    await producer.send_event(click_event)
    producer.flush()
    producer.close()
