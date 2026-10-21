import random
from typing import List, Dict, Any
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.timestamps import now_iso
from simulator.utils.random_utils import random_id, generate_amount

class NormalScenario:
    """Mô phỏng giao dịch mua sắm bình thường theo thói quen của người dùng."""

    def __init__(self, merchant_catalog: MerchantCatalog):
        self.catalog = merchant_catalog

    def generate(self, user: User) -> TransactionEvent:
        card = user.primary_card
        
        # Chọn merchant theo category quen thuộc của user
        cat = random.choice(user.preferred_categories) if user.preferred_categories else None
        merchant = self.catalog.get_random_merchant(category=cat, country=user.home_country)

        # Số tiền dao động quanh mức trung bình thường ngày
        amount = generate_amount(
            mean=user.avg_transaction_amount,
            std=user.avg_transaction_amount * 0.3,
            min_val=20000.0
        )
        # Giới hạn không vượt quá mức tối đa thông thường
        amount = min(amount, user.max_usual_amount)

        channel = random.choice(["mobile", "web", "pos"])
        method = "card" if channel != "mobile" else random.choice(["card", "e_wallet", "qr_code"])

        # Cập nhật state thẻ
        card.last_country = user.home_country
        card.last_ip = user.home_ip

        return TransactionEvent(
            event_id=random_id("evt_"),
            event_type="transaction",
            event_time=now_iso(),
            transaction_id=random_id("txn_"),
            user_id=user.user_id,
            card_id=card.card_id,
            merchant_id=merchant.merchant_id,
            amount=amount,
            currency="VND",
            country=user.home_country,
            channel=channel,
            payment_method=method,
            device_id=user.device_id,
            ip_address=user.home_ip,
            status="success",
            is_fraud=0,
            fraud_scenario=None,
        )

