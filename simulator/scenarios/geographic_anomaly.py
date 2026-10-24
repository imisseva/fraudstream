import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.timestamps import now_iso
from simulator.utils.random_utils import random_id, random_ip

class GeographicAnomalyScenario:
    """Scenario C: Geographic Anomaly / Impossible Travel — Cùng 1 thẻ xuất hiện ở 2 quốc gia cách xa nhau trong thời gian ngắn."""

    def __init__(self, merchant_catalog: MerchantCatalog, countries: List[Dict[str, Any]]):
        self.catalog = merchant_catalog
        self.countries = countries

    def generate_sequence(self, user: User) -> List[TransactionEvent]:
        """Phát 1 cặp 2 giao dịch: 1 giao dịch hợp lệ tại VN, sau đó 5-15 phút xuất hiện giao dịch nước ngoài."""
        card = user.primary_card
        now = datetime.now(timezone.utc)
        
        # 1. Giao dịch hợp lệ tại Việt Nam (xảy ra trước đó 5-15 phút)
        domestic_time = now - timedelta(minutes=random.uniform(5.0, 15.0))
        vn_merchant = self.catalog.get_random_merchant(category=random.choice(["supermarket", "gas_station", "coffee"]))
        vn_amount = float(random.choice([45000, 120000, 350000, 850000]))

        legit_event = TransactionEvent(
            event_id=random_id("evt_"),
            event_type="transaction",
            event_time=domestic_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            transaction_id=random_id("txn_"),
            user_id=user.user_id,
            card_id=card.card_id,
            merchant_id=vn_merchant.merchant_id,
            amount=vn_amount,
            currency="VND",
            country=user.home_country,  # "VN"
            channel="pos",
            payment_method="card",
            device_id=user.device_id,
            ip_address=user.home_ip,
            status="success",
            is_fraud=0,
            fraud_scenario=None,
        )

        # 2. Giao dịch gian lận tại nước ngoài (xảy ra ngay thời điểm now)
        foreign_candidates = [c for c in self.countries if c["code"] != user.home_country]
        foreign_country = random.choice(foreign_candidates)
        subnets = foreign_country.get("ip_subnets", ["198.51.100.0"])
        foreign_ip = random_ip(random.choice(subnets))
        foreign_merchant = self.catalog.get_random_merchant(country=foreign_country["code"])
        fraud_amount = float(random.choice([1500000, 3200000, 8500000, 15000000]))

        card.last_country = foreign_country["code"]
        card.last_ip = foreign_ip

        fraud_event = TransactionEvent(
            event_id=random_id("evt_"),
            event_type="transaction",
            event_time=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            transaction_id=random_id("txn_"),
            user_id=user.user_id,
            card_id=card.card_id,
            merchant_id=foreign_merchant.merchant_id,
            amount=fraud_amount,
            currency="VND",
            country=foreign_country["code"],  # Khác home_country!
            channel="web",
            payment_method="card",
            device_id=user.device_id,
            ip_address=foreign_ip,
            status="success",
            is_fraud=1,
            fraud_scenario="geographic_anomaly",
        )

        return [legit_event, fraud_event]

    def generate(self, user: User) -> TransactionEvent:
        """Tương thích ngược: trả về sự kiện gian lận của cặp Sequence."""
        pair = self.generate_sequence(user)
        return pair[1]

