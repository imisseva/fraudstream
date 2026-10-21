import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from simulator.config import SimulatorConfig, KafkaConfig
from simulator.models.user import User, UserType
from simulator.models.product import ProductCatalog
from simulator.models.merchant import MerchantCatalog
from simulator.models.session import Session
from simulator.generators.user_generator import UserGenerator
from simulator.generators.transaction_generator import TransactionGenerator
from simulator.generators.clickstream_generator import ClickstreamGenerator
from simulator.generators.traffic_pattern import TrafficPattern
from simulator.producers.kafka_producer import KafkaEventProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fraudstream.simulator")


class FraudStreamSimulator:
    def __init__(self, config: SimulatorConfig):
        self.config = config
        self.running = False
        self.stats = {
            "total_events": 0,
            "transactions": 0,
            "fraud_transactions": 0,
            "clickstream_events": 0,
            "data_quality_glitches": 0,
        }
        
        # Load reference data
        data_dir = Path(__file__).parent / "data"
        self.catalog = ProductCatalog(data_dir / "products.json")
        self.merchant_catalog = MerchantCatalog(data_dir / "merchants.json")
        
        with open(data_dir / "cities.json", "r", encoding="utf-8") as f:
            self.cities = json.load(f)
        with open(data_dir / "devices.json", "r", encoding="utf-8") as f:
            self.devices = json.load(f)
        with open(data_dir / "countries.json", "r", encoding="utf-8") as f:
            self.countries = json.load(f)

        # Components
        self.user_gen = UserGenerator(config, self.countries, self.cities, self.devices)
        self.txn_gen = TransactionGenerator(config, self.merchant_catalog, self.countries)
        self.click_gen = ClickstreamGenerator(config, self.catalog)
        self.traffic = TrafficPattern(config.base_events_per_second)
        
        self.producer = KafkaEventProducer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            topic_transactions=config.kafka.topic_transactions,
            topic_clickstream=config.kafka.topic_clickstream,
            security_protocol=config.kafka.security_protocol,
            sasl_mechanism=config.kafka.sasl_mechanism,
            sasl_username=config.kafka.sasl_username,
            sasl_password=config.kafka.sasl_password,
            dry_run=config.dry_run,
        )

        # Create user pool
        self.users = self.user_gen.generate_user_pool()
        logger.info(
            f"Created {len(self.users)} users "
            f"(normal: {sum(1 for u in self.users if u.user_type == UserType.NORMAL)}, "
            f"suspicious: {sum(1 for u in self.users if u.user_type == UserType.SUSPICIOUS)}, "
            f"fraudster: {sum(1 for u in self.users if u.user_type == UserType.FRAUDSTER)})"
        )
        logger.info(
            f"Loaded {len(self.merchant_catalog.merchants)} merchants and "
            f"{len(self.countries)} countries reference data."
        )

    def _create_session(self, user: User) -> Session:
        """Tạo session clickstream mới cho user."""
        return Session(
            ip_address=user.home_ip,
            device=user.primary_device,
            city=user.home_city,
        )

    async def run(self, max_events: Optional[int] = None):
        """Main loop — chạy liên tục hoặc dừng khi đạt max_events."""
        self.running = True
        logger.info("🚀 FraudStream Multi-Scenario Simulator started!")
        if self.config.dry_run:
            logger.info("🧪 Running in DRY-RUN mode (events generated without Kafka broker)")
        else:
            logger.info(
                f"📡 Streaming to Kafka topics: "
                f"transactions='{self.config.kafka.topic_transactions}', "
                f"clickstream='{self.config.kafka.topic_clickstream}' "
                f"at {self.config.kafka.bootstrap_servers}"
            )

        try:
            while self.running:
                if max_events is not None and self.stats["total_events"] >= max_events:
                    logger.info(f"Reached maximum events limit ({max_events}). Stopping.")
                    break

                # Chọn random user
                user = random.choice(self.users)

                # Quyết định sinh Transaction Event hay Clickstream Event
                is_transaction = random.random() < self.config.transaction_ratio
                event = None

                if is_transaction:
                    event = self.txn_gen.generate_next_event(user)
                    self.stats["transactions"] += 1
                    if event.get("is_fraud") == 1:
                        self.stats["fraud_transactions"] += 1
                    if "_data_quality_glitch" in event:
                        self.stats["data_quality_glitches"] += 1
                else:
                    if user.current_session is None or not user.current_session.is_active():
                        user.current_session = self._create_session(user)
                    click_event = self.click_gen.generate_next_event(user, user.current_session)
                    if click_event:
                        event = click_event.to_dict()
                        self.stats["clickstream_events"] += 1

                # Gửi vào Kafka nếu có event
                if event:
                    await self.producer.send_event(event)
                    self.stats["total_events"] += 1

                    # Log progress định kỳ
                    log_interval = 100 if self.config.base_events_per_second >= 10 else 10
                    if self.stats["total_events"] % log_interval == 0:
                        logger.info(
                            f"📊 Total: {self.stats['total_events']} | "
                            f"Txns: {self.stats['transactions']} (Fraud: {self.stats['fraud_transactions']}) | "
                            f"Clicks: {self.stats['clickstream_events']} | "
                            f"Glitches: {self.stats['data_quality_glitches']} | "
                            f"Rate: {self.traffic.get_current_rate():.1f} evt/s"
                        )

                # Sleep theo traffic pattern
                await asyncio.sleep(self.traffic.get_sleep_interval())
        except asyncio.CancelledError:
            pass
        finally:
            self.stop()

    def stop(self):
        if not self.running:
            return
        self.running = False
        self.producer.close()
        logger.info("🛑 Simulator stopped.")
        logger.info(f"📊 Final stats: {self.stats}")


def parse_args():
    parser = argparse.ArgumentParser(description="FraudStream Multi-Scenario Financial & Clickstream Simulator")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka broker servers",
    )
    parser.add_argument(
        "--topic-transactions",
        type=str,
        default=os.environ.get("TOPIC_TRANSACTIONS", "transaction-events"),
        help="Kafka topic for transactions",
    )
    parser.add_argument(
        "--topic-clickstream",
        type=str,
        default=os.environ.get("TOPIC_CLICKSTREAM", "clickstream-events"),
        help="Kafka topic for clickstream events",
    )
    parser.add_argument(
        "--security-protocol",
        type=str,
        default=os.environ.get("SECURITY_PROTOCOL"),
        help="Kafka security protocol (e.g. SASL_SSL)",
    )
    parser.add_argument(
        "--sasl-mechanism",
        type=str,
        default=os.environ.get("SASL_MECHANISM"),
        help="SASL mechanism (e.g. SCRAM-SHA-256 or PLAIN)",
    )
    parser.add_argument(
        "--sasl-username",
        type=str,
        default=os.environ.get("SASL_USERNAME"),
        help="SASL username / API key",
    )
    parser.add_argument(
        "--sasl-password",
        type=str,
        default=os.environ.get("SASL_PASSWORD"),
        help="SASL password / API secret",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("RATE", "20.0")),
        help="Base events per second",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=int(os.environ.get("USERS", "1000")),
        help="Total number of simulated users",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes"),
        help="Run without sending messages to Kafka broker",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=int(os.environ["MAX_EVENTS"]) if os.environ.get("MAX_EVENTS") else None,
        help="Stop after generating N events",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = SimulatorConfig(
        total_users=args.users,
        base_events_per_second=args.rate,
        dry_run=args.dry_run,
        kafka=KafkaConfig(
            bootstrap_servers=args.bootstrap_servers,
            topic_transactions=args.topic_transactions,
            topic_clickstream=args.topic_clickstream,
            security_protocol=args.security_protocol,
            sasl_mechanism=args.sasl_mechanism,
            sasl_username=args.sasl_username,
            sasl_password=args.sasl_password,
        ),
    )
    simulator = FraudStreamSimulator(config)

    # Graceful shutdown handlers
    def handle_signal(sig, frame):
        simulator.stop()
        sys.exit(0)

    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except (ValueError, AttributeError):
        pass

    try:
        asyncio.run(simulator.run(max_events=args.max_events))
    except KeyboardInterrupt:
        simulator.stop()


if __name__ == "__main__":
    main()
