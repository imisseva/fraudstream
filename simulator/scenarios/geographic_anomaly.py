import random
from typing import List, Dict, Any
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.timestamps import now_iso
from simulator.utils.random_utils import random_id, random_ip

class GeographicAnomalyScenario:
    """Scenario C: Geographic Anomaly / Impossible Travel — Cùng 1 thẻ xuất hiện ở 2 quốc gia cách xa nhau."""

    def __init__(self, merchant_catalog: MerchantCatalog, countries: List[Dict[str, Any]]):
        self.catalog = merchant_catalog
        self.countries = countries

    def generate(self, user: User) -> TransactionEvent:
        card = user.primary_card
        
        # Chọn 1 quốc gia khác hẳn với quốc gia thường trú (home_country)
        foreign_candidates = [c for c in self.countries if c["code"] != user.home_country]
        foreign_country = random.choice(foreign_candidates)

        # Lấy IP tương ứng với quốc gia ngoại
        subnets = foreign_country.get("ip_subnets", ["198.51.100.0"])
        foreign_ip = random_ip(random.choice(subnets))

        # Merchant quốc tế
        merchant = self.catalog.get_random_merchant(country=foreign_country["code"])

        amount = float(random.choice([1500000, 3200000, 8500000, 15000000]))

        # Cập nhật state thẻ là đã bị dùng ở nước ngoài
        card.last_country = foreign_country["code"]
        card.last_ip = foreign_ip

        return TransactionEvent(
            event_id=random_id("evt_"),
            event_type="transaction",
            event_time=now_iso(),
            transaction_id=random_id("txn_"),
            user_id=user.user_id,
            card_id=card.card_id,
            merchant_id=merchant.merchant_id,
            amount=amount,
            currency="VND",  # Đã quy đổi hoặc billed bằng VND
            country=foreign_country["code"],  # Khác home_country!
            channel="web",
            payment_method="card",
            device_id=user.device_id,
            ip_address=foreign_ip,
            status="success",
            is_fraud=1,
            fraud_scenario="geographic_anomaly",
        )

