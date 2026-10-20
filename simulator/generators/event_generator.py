import uuid
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from simulator.models.session import Session, SessionStage
from simulator.models.user import User
from simulator.models.product import ProductCatalog
from simulator.config import SimulatorConfig

class EventGenerator:
    def __init__(self, config: SimulatorConfig, product_catalog: ProductCatalog):
        self.config = config
        self.catalog = product_catalog

    def generate_next_event(self, user: User, session: Session) -> Optional[Dict[str, Any]]:
        """Sinh event tiếp theo dựa vào stage hiện tại"""
        
        base_event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.user_id,
            "session_id": session.session_id,
            "ip_address": session.ip_address,
            "device": session.device,
            "city": session.city,
            "user_type": user.user_type.value,  # Để verify fraud detection sau này
        }

        if session.stage == SessionStage.LANDING:
            return self._gen_page_view(base_event, session)
        elif session.stage == SessionStage.BROWSING:
            return self._gen_browsing_event(base_event, user, session)
        elif session.stage == SessionStage.PRODUCT_VIEWING:
            return self._gen_product_action(base_event, user, session)
        elif session.stage == SessionStage.CARTING:
            return self._gen_cart_action(base_event, user, session)
        else:
            return None

    def _gen_page_view(self, base: Dict[str, Any], session: Session) -> Dict[str, Any]:
        session.stage = SessionStage.BROWSING
        session.events_count += 1
        return {
            **base,
            "event_type": "page_view",
            "page": "/",
            "referrer": random.choice(["google.com", "facebook.com", "direct", ""]),
        }

    def _gen_browsing_event(self, base: Dict[str, Any], user: User, session: Session) -> Dict[str, Any]:
        # 60% search, 40% xem product trực tiếp
        if random.random() < self.config.search_after_pageview:
            session.events_count += 1
            query = random.choice(user.preferred_categories) if user.preferred_categories else "electronics"
            return {
                **base,
                "event_type": "search",
                "query": query,
                "results_count": random.randint(5, 50),
            }
        else:
            return self._gen_product_view(base, user, session)

    def _gen_product_view(self, base: Dict[str, Any], user: User, session: Session) -> Dict[str, Any]:
        category = random.choice(user.preferred_categories) if user.preferred_categories else None
        product = self.catalog.get_random_product(category=category)
        session.stage = SessionStage.PRODUCT_VIEWING
        session.events_count += 1
        session._current_product = product  # Lưu tạm để add_to_cart
        return {
            **base,
            "event_type": "product_view",
            "product_id": product.product_id,
            "product_name": product.name,
            "category": product.category,
            "price": product.price,
        }

    def _gen_product_action(self, base: Dict[str, Any], user: User, session: Session) -> Dict[str, Any]:
        # 35% add to cart, 65% tiếp tục browse
        if random.random() < self.config.add_to_cart_rate:
            product = getattr(session, "_current_product", None) or self.catalog.get_random_product()
            session.cart_items.append(product)
            session.cart_total += product.price
            session.stage = SessionStage.CARTING
            session.events_count += 1
            return {
                **base,
                "event_type": "add_to_cart",
                "product_id": product.product_id,
                "product_name": product.name,
                "price": product.price,
                "quantity": 1,
                "cart_total": session.cart_total,
            }
        else:
            session.stage = SessionStage.BROWSING
            return self._gen_product_view(base, user, session)

    def _gen_cart_action(self, base: Dict[str, Any], user: User, session: Session) -> Optional[Dict[str, Any]]:
        # 30% purchase, 70% abandon
        if random.random() < self.config.purchase_rate:
            session.stage = SessionStage.COMPLETED
            session.events_count += 1
            order_id = f"ORD_{uuid.uuid4().hex[:8]}"
            return {
                **base,
                "event_type": "purchase",
                "order_id": order_id,
                "items": [
                    {
                        "product_id": p.product_id,
                        "name": p.name,
                        "price": p.price,
                        "qty": 1,
                    }
                    for p in session.cart_items
                ],
                "total_amount": session.cart_total,
                "payment_method": random.choice(
                    ["credit_card", "debit_card", "bank_transfer", "e_wallet"]
                ),
            }
        else:
            session.stage = SessionStage.ABANDONED
            return None  # User bỏ giỏ hàng, không sinh event

