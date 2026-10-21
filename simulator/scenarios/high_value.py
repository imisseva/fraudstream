import random
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.timestamps import now_iso
from simulator.utils.random_utils import random_id

class HighValueScenario:
    """Scenario A: Giao dịch có giá trị đột biến so với lịch sử chi tiêu của User."""

    def __init__(self, merchant_catalog: MerchantCatalog):
        self.catalog = merchant_catalog

    def generate(self, user: User) -> TransactionEvent:
        card = user.primary_card
        
        # Mua hàng tại merchant rủi ro cao: Vàng bạc đá quý, đồ xa xỉ, điện thoại đắt tiền, crypto
        high_risk_merchants = [m for m in self.catalog.merchants if m.category in ("jewelry", "crypto", "electronics")]
        merchant = random.choice(high_risk_merchants) if high_risk_merchants else self.catalog.get_high_risk_merchant()

        # Số tiền gấp 8x - 25x so với mức tối đa thông thường
        multiplier = random.uniform(8.0, 25.0)
        spike_amount = round(user.max_usual_amount * multiplier, -4)
        spike_amount = max(spike_amount, 25000000.0)  # Tối thiểu 25 triệu VND

        return TransactionEvent(
            event_id=random_id("evt_"),
            event_type="transaction",
            event_time=now_iso(),
            transaction_id=random_id("txn_"),
            user_id=user.user_id,
            card_id=card.card_id,
            merchant_id=merchant.merchant_id,
            amount=spike_amount,
            currency="VND",
            country=user.home_country,
            channel=random.choice(["mobile", "web"]),
            payment_method="card",
            device_id=user.device_id,
            ip_address=user.home_ip,
            status="success",
            is_fraud=1,
            fraud_scenario="high_value",
        )

