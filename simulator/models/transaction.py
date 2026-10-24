from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class TransactionEvent:
    event_id: str
    event_type: str = "transaction"
    event_time: str = ""
    transaction_id: str = ""
    user_id: str = ""
    card_id: str = ""
    merchant_id: str = ""
    amount: float = 0.0
    currency: str = "VND"
    country: str = "VN"
    channel: str = "mobile"         # mobile, web, pos
    payment_method: str = "card"    # card, e_wallet, qr_code
    device_id: str = ""
    ip_address: str = ""
    status: str = "success"         # success, failed, pending
    is_fraud: int = 0               # 0 (normal) or 1 (fraud)
    fraud_scenario: Optional[str] = None  # e.g., 'high_value', 'velocity', 'geographic_anomaly', 'device_ip_sharing', 'repeated_attempts'
    session_id: Optional[str] = None      # Correlation ID liên kết với clickstream session

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if self.fraud_scenario is None:
            d.pop("fraud_scenario", None)
        return d

