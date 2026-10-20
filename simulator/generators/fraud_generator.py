import uuid
import random
from datetime import datetime, timezone
from typing import Dict, Any, List
from simulator.models.session import Session, SessionStage
from simulator.models.user import User
from simulator.models.product import ProductCatalog

class FraudGenerator:
    """Sinh fraud patterns cho fraudster users"""

    def __init__(self, product_catalog: ProductCatalog, cities: List[Dict[str, str]]):
        self.catalog = product_catalog
        self.cities = cities

    def generate_fraud_event(self, user: User, session: Session) -> Dict[str, Any]:
        """Chọn random 1 fraud scenario"""
        scenario = random.choice(["card_testing", "account_takeover", "bot_attack"])
        
        if scenario == "card_testing":
            return self._card_testing(user, session)
        elif scenario == "account_takeover":
            return self._account_takeover(user, session)
        else:
            return self._bot_attack(user, session)

    def _card_testing(self, user: User, session: Session) -> Dict[str, Any]:
        """Mua nhiều gift card giá nhỏ tăng dần"""
        product = self.catalog.get_high_risk_product()
        amount = random.choice([10000, 50000, 100000, 500000, 1000000])
        
        session.stage = SessionStage.COMPLETED
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "purchase",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.user_id,
            "session_id": session.session_id,
            "ip_address": session.ip_address,
            "device": session.device,
            "city": session.city,
            "user_type": "fraudster",
            "order_id": f"ORD_{uuid.uuid4().hex[:8]}",
            "items": [
                {
                    "product_id": product.product_id,
                    "name": product.name,
                    "price": amount,
                    "qty": 1,
                }
            ],
            "total_amount": amount,
            "payment_method": "credit_card",
            "fraud_scenario": "card_testing",  # Label để verify
        }

    def _account_takeover(self, user: User, session: Session) -> Dict[str, Any]:
        """Đổi IP + device đột ngột, mua sản phẩm đắt tiền"""
        new_city = random.choice(self.cities)
        
        # IP mới từ dải IP của thành phố khác
        parts = new_city["ip_range"].split(".")
        if len(parts) == 4 and parts[3] == "0":
            session.ip_address = f"{parts[0]}.{parts[1]}.{parts[2]}.{random.randint(2, 254)}"
        else:
            session.ip_address = new_city["ip_range"]
            
        session.city = new_city["name"]
        session.device = {
            "type": random.choice(["desktop", "mobile"]),
            "os": random.choice(["Windows", "Linux"]),
            "browser": random.choice(["Chrome", "Firefox"]),
            "fingerprint": f"fp_{uuid.uuid4().hex[:6]}",  # Device mới hoàn toàn
        }
        
        product = self.catalog.get_high_risk_product()
        session.stage = SessionStage.COMPLETED
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "purchase",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.user_id,
            "session_id": session.session_id,
            "ip_address": session.ip_address,
            "device": session.device,
            "city": session.city,
            "user_type": "fraudster",
            "order_id": f"ORD_{uuid.uuid4().hex[:8]}",
            "items": [
                {
                    "product_id": product.product_id,
                    "name": product.name,
                    "price": product.price,
                    "qty": 1,
                }
            ],
            "total_amount": product.price,
            "payment_method": "credit_card",
            "fraud_scenario": "account_takeover",
        }

    def _bot_attack(self, user: User, session: Session) -> Dict[str, Any]:
        """Nhiều product_view liên tục, không browse bình thường"""
        product = self.catalog.get_random_product()
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "product_view",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.user_id,
            "session_id": session.session_id,
            "ip_address": session.ip_address,
            "device": session.device,
            "city": session.city,
            "user_type": "fraudster",
            "product_id": product.product_id,
            "product_name": product.name,
            "category": product.category,
            "price": product.price,
            "fraud_scenario": "bot_attack",
        }

