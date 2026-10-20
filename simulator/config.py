from dataclasses import dataclass, field
from typing import Optional

# Traffic multiplier theo giờ trong ngày (0-23)
HOURLY_TRAFFIC = {
    0: 0.1,  1: 0.05, 2: 0.05, 3: 0.03,
    4: 0.05, 5: 0.1,  6: 0.2,  7: 0.4,
    8: 0.6,  9: 0.7,  10: 0.8, 11: 0.9,
    12: 1.0, 13: 0.85, 14: 0.7, 15: 0.6,
    16: 0.65, 17: 0.7, 18: 0.8, 19: 0.9,
    20: 1.0,  21: 1.0, 22: 0.8, 23: 0.5,
}

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    topic: str = "clickstream-events"

@dataclass
class SimulatorConfig:
    # User pool
    total_users: int = 1000
    normal_user_ratio: float = 0.80      # 800 users
    suspicious_user_ratio: float = 0.15  # 150 users
    fraudster_ratio: float = 0.05        # 50 users

    # Traffic
    base_events_per_second: float = 20.0
    
    # Funnel conversion rates
    search_after_pageview: float = 0.60
    product_view_after_search: float = 0.70
    add_to_cart_rate: float = 0.35
    purchase_rate: float = 0.30          # 30% hoàn tất checkout

    # Kafka & Dry run mode
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    dry_run: bool = False

    def __post_init__(self):
        if self.kafka is None:
            self.kafka = KafkaConfig()

