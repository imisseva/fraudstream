import random
from datetime import datetime, timezone, timedelta
from typing import List
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.random_utils import random_id

class RepeatedAttemptsScenario:
    """Scenario E: Repeated Attempts — Nhiều lần thanh toán thất bại (failed) rồi xuất hiện 1 giao dịch thành công (success)."""

    def __init__(self, merchant_catalog: MerchantCatalog):
        self.catalog = merchant_catalog

    def generate_sequence(self, user: User, failure_count: int = 4) -> List[TransactionEvent]:
        card = user.primary_card
        merchant = self.catalog.get_random_merchant(category=random.choice(["electronics", "gaming", "jewelry"]))
        base_time = datetime.now(timezone.utc)
        events: List[TransactionEvent] = []

        # Các lần thử thất bại liên tiếp (sai CVV/OTP/vượt hạn mức)
        for i in range(failure_count):
            txn_time = base_time + timedelta(seconds=i * random.uniform(2.0, 5.0))
            test_amount = float(random.choice([50000, 100000, 500000]))

            failed_event = TransactionEvent(
                event_id=random_id("evt_"),
                event_type="transaction",
                event_time=txn_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                transaction_id=random_id("txn_"),
                user_id=user.user_id,
                card_id=card.card_id,
                merchant_id=merchant.merchant_id,
                amount=test_amount,
                currency="VND",
                country=user.home_country,
                channel="web",
                payment_method="card",
                device_id=user.device_id,
                ip_address=user.home_ip,
                status="failed",                 # Thất bại!
                is_fraud=1,
                fraud_scenario="repeated_attempts",
            )
            events.append(failed_event)

        # Lần cuối cùng thành công với số tiền lớn
        success_time = base_time + timedelta(seconds=failure_count * 5.0 + 3.0)
        final_amount = float(random.choice([5000000, 12000000, 25000000]))
        
        success_event = TransactionEvent(
            event_id=random_id("evt_"),
            event_type="transaction",
            event_time=success_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            transaction_id=random_id("txn_"),
            user_id=user.user_id,
            card_id=card.card_id,
            merchant_id=merchant.merchant_id,
            amount=final_amount,
            currency="VND",
            country=user.home_country,
            channel="web",
            payment_method="card",
            device_id=user.device_id,
            ip_address=user.home_ip,
            status="success",                # Cuối cùng thành công!
            is_fraud=1,
            fraud_scenario="repeated_attempts",
        )
        events.append(success_event)

        return events

