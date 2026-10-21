import random
from typing import List
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.timestamps import now_iso
from simulator.utils.random_utils import random_id

class DeviceIpSharingScenario:
    """Scenario D: Device/IP Sharing — Nhiều tài khoản/thẻ khác nhau dùng chung 1 thiết bị hoặc IP (Fraud Ring)."""

    def __init__(self, merchant_catalog: MerchantCatalog):
        self.catalog = merchant_catalog
        # Thiết bị và IP cố định mà nhóm gian lận dùng chung
        self.shared_device_id = "dev_fraud_farm_09"
        self.shared_ip_address = "103.12.99.145"

    def generate(self, user: User) -> TransactionEvent:
        card = user.primary_card
        merchant = self.catalog.get_random_merchant(category="online_retail")
        amount = float(random.choice([800000, 1500000, 3000000, 6000000]))

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
            channel="web",
            payment_method="card",
            device_id=self.shared_device_id,     # Cùng 1 thiết bị dùng chung cho nhiều users!
            ip_address=self.shared_ip_address,   # Cùng 1 IP dùng chung!
            status="success",
            is_fraud=1,
            fraud_scenario="device_ip_sharing",
        )

