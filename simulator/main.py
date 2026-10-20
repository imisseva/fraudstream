import argparse
import asyncio
import json
import logging
import random
import signal
import sys
from pathlib import Path
from typing import List, Optional

from simulator.config import SimulatorConfig, KafkaConfig
from simulator.models.user import User, UserType
from simulator.models.product import ProductCatalog
from simulator.models.session import Session
from simulator.generators.event_generator import EventGenerator
from simulator.generators.fraud_generator import FraudGenerator
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
        self.stats = {"total_events": 0, "fraud_events": 0, "purchases": 0}
        
        # Load data
        data_dir = Path(__file__).parent / "data"
        self.catalog = ProductCatalog(data_dir / "products.json")
        
        with open(data_dir / "cities.json", "r", encoding="utf-8") as f:
            self.cities = json.load(f)
        with open(data_dir / "devices.json", "r", encoding="utf-8") as f:
            self.devices = json.load(f)

        # Components
        self.event_gen = EventGenerator(config, self.catalog)
        self.fraud_gen = FraudGenerator(self.catalog, self.cities)
        self.traffic = TrafficPattern(config.base_events_per_second)
        self.producer = KafkaEventProducer(
            bootstrap_servers=config.kafka.bootstrap_servers, 
            topic=config.kafka.topic,
            dry_run=config.dry_run,
        )

        # Create user pool
        self.users = self._create_user_pool()
        logger.info(
            f"Created {len(self.users)} users "
            f"(normal: {sum(1 for u in self.users if u.user_type == UserType.NORMAL)}, "
            f"suspicious: {sum(1 for u in self.users if u.user_type == UserType.SUSPICIOUS)}, "
            f"fraudster: {sum(1 for u in self.users if u.user_type == UserType.FRAUDSTER)})"
        )

    def _create_user_pool(self) -> List[User]:
        users = []
        n = self.config.total_users
        
        for _ in range(int(n * self.config.normal_user_ratio)):
            users.append(User.create_normal(self.cities, self.devices))
        for _ in range(int(n * self.config.suspicious_user_ratio)):
            users.append(User.create_suspicious(self.cities, self.devices))
        for _ in range(int(n * self.config.fraudster_ratio)):
            users.append(User.create_fraudster(self.cities, self.devices))
        
        return users

    def _create_session(self, user: User) -> Session:
        """Tạo session mới cho user"""
        if user.user_type == UserType.NORMAL:
            ip = user.home_ip
            device = user.primary_device
            city = user.home_city
        else:
            # Suspicious/Fraud có thể đổi IP/device
            chosen_city = random.choice(self.cities)
            ip = User._generate_ip(chosen_city["ip_range"])
            device = random.choice(self.devices)
            city = chosen_city["name"]

        return Session(
            ip_address=ip,
            device=device,
            city=city,
        )

    async def run(self, max_events: Optional[int] = None):
        """Main loop — chạy liên tục hoặc dừng khi đạt max_events"""
        self.running = True
        logger.info("🚀 FraudStream Simulator started!")
        if self.config.dry_run:
            logger.info("🧪 Running in DRY-RUN mode (events generated without Kafka broker)")
        else:
            logger.info(f"📡 Sending events to Kafka topic: {self.config.kafka.topic} ({self.config.kafka.bootstrap_servers})")
        
        try:
            while self.running:
                if max_events is not None and self.stats["total_events"] >= max_events:
                    logger.info(f"Reached maximum events limit ({max_events}). Stopping.")
                    break

                # Chọn random user
                user = random.choice(self.users)
                
                # Tạo session mới nếu chưa có hoặc session cũ đã kết thúc
                if user.current_session is None or not user.current_session.is_active():
                    user.current_session = self._create_session(user)

                # Sinh event
                event = None
                if user.user_type == UserType.FRAUDSTER and random.random() < 0.6:
                    event = self.fraud_gen.generate_fraud_event(
                        user, user.current_session
                    )
                    if event:
                        self.stats["fraud_events"] += 1
                else:
                    event = self.event_gen.generate_next_event(
                        user, user.current_session
                    )

                # Gửi vào Kafka
                if event:
                    await self.producer.send_event(event)
                    self.stats["total_events"] += 1
                    
                    if event.get("event_type") == "purchase":
                        self.stats["purchases"] += 1

                    # Log mỗi 100 events (hoặc 10 events nếu base_rate thấp)
                    log_interval = 100 if self.config.base_events_per_second >= 10 else 10
                    if self.stats["total_events"] % log_interval == 0:
                        logger.info(
                            f"📊 Events: {self.stats['total_events']} | "
                            f"Purchases: {self.stats['purchases']} | "
                            f"Fraud: {self.stats['fraud_events']} | "
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
    parser = argparse.ArgumentParser(description="FraudStream Clickstream & Fraud Simulator")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka broker servers")
    parser.add_argument("--topic", type=str, default="clickstream-events", help="Kafka topic name")
    parser.add_argument("--rate", type=float, default=20.0, help="Base events per second")
    parser.add_argument("--users", type=int, default=1000, help="Total number of simulated users")
    parser.add_argument("--dry-run", action="store_true", help="Run without sending messages to Kafka broker")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after generating N events")
    return parser.parse_args()


def main():
    args = parse_args()
    config = SimulatorConfig(
        total_users=args.users,
        base_events_per_second=args.rate,
        dry_run=args.dry_run,
        kafka=KafkaConfig(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
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

