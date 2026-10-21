import random
from datetime import datetime, timezone, timedelta
from typing import List
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.random_utils import random_id

class VelocityScenario:
    """Scenario B: Transaction Velocity — 8-15 giao dịch dồn dập trong vòng dưới 1 phút trên cùng 1 thẻ."""

    def __init__(self, merchant_catalog: MerchantCatalog):
        self.catalog = merchant_catalog

    def generate_burst(self, user: User, burst_count: int = 10) -> List[TransactionEvent]:
        card = user.primary_card
        base_time = datetime.now(timezone.utc)
        events: List[TransactionEvent] = []

        # Các giao dịch velocity thường có giá trị nhỏ đến vừa (gift cards, gaming topup)
        for i in range(burst_count):
            merchant = self.catalog.get_random_merchant(category=random.choice(["gaming", "online_retail", "supermarket"]))
            txn_time = base_time + timedelta(seconds=i * random.uniform(1.0, 4.0))
            amount = float(random.choice([50000, 100000, 200000, 500000]))

            event = TransactionEvent(
                event_id=random_id("evt_"),
                event_type="transaction",
                event_time=txn_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                transaction_id=random_id("txn_"),
                user_id=user.user_id,
                card_id=card.card_id,
                merchant_id=merchant.merchant_id,
                amount=amount,
                currency="VND",
                country=user.home_country,
                channel="web",
                payment_method="card",
                device_id=user.device_id,
                ip_address=user.home_ip,
                status="success",
                is_fraud=1,
                fraud_scenario="velocity",
            )
            events.append(event)

        return events

