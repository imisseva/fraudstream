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
    topic_transactions: str = "transaction-events"
    topic_clickstream: str = "clickstream-events"
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None

    @property
    def topic(self) -> str:
        """Thuộc tính tương thích ngược."""
        return self.topic_transactions

@dataclass
class SimulatorConfig:
    # User pool
    total_users: int = 1000
    normal_user_ratio: float = 0.80      # 800 users
    suspicious_user_ratio: float = 0.15  # 150 users
    fraudster_ratio: float = 0.05        # 50 users

    # Traffic
    base_events_per_second: float = 20.0
    
    # Phân bổ loại event
    transaction_ratio: float = 0.60      # 60% transactions
    clickstream_ratio: float = 0.40      # 40% clickstream

    # Fraud rates & scenario weights
    fraud_rate: float = 0.05             # 5% transaction thông thường bị fraud
    scenario_a_weight: float = 0.25      # High value
    scenario_b_weight: float = 0.20      # Velocity
    scenario_c_weight: float = 0.20      # Geographic anomaly
    scenario_d_weight: float = 0.15      # Device/IP sharing
    scenario_e_weight: float = 0.20      # Repeated attempts

    # Data Quality glitches
    data_quality_error_rate: float = 0.02 # 2% events chứa dữ liệu lỗi (sai type, trễ, thiếu trường...)

    # Funnel conversion rates
    search_after_pageview: float = 0.60
    product_view_after_search: float = 0.70
    add_to_cart_rate: float = 0.35
    purchase_rate: float = 0.30

    # Kafka & Dry run mode
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    dry_run: bool = False

    def __post_init__(self):
        if self.kafka is None:
            self.kafka = KafkaConfig()
