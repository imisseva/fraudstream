import random
from typing import Dict, Any, Optional
from simulator.models.user import User
from simulator.models.session import Session, SessionStage
from simulator.models.product import ProductCatalog
from simulator.models.clickstream import ClickstreamEvent
from simulator.config import SimulatorConfig
from simulator.utils.timestamps import now_iso
from simulator.utils.random_utils import random_id

class ClickstreamGenerator:
    """Sinh luồng sự kiện hành vi người dùng (Clickstream) theo phễu mua hàng."""

    def __init__(self, config: SimulatorConfig, product_catalog: ProductCatalog):
        self.config = config
        self.catalog = product_catalog

    def generate_next_event(self, user: User, session: Session) -> Optional[ClickstreamEvent]:
        if session.stage == SessionStage.LANDING:
            return self._gen_page_view(user, session)
        elif session.stage == SessionStage.BROWSING:
            return self._gen_browsing_event(user, session)
        elif session.stage == SessionStage.PRODUCT_VIEWING:
            return self._gen_product_action(user, session)
        elif session.stage == SessionStage.CARTING:
            return self._gen_cart_action(user, session)
        else:
            return None

    def _gen_page_view(self, user: User, session: Session) -> ClickstreamEvent:
        session.stage = SessionStage.BROWSING
        session.events_count += 1
        return ClickstreamEvent(
            event_id=random_id("evt_"),
            event_type="page_view",
            event_time=now_iso(),
            user_id=user.user_id,
            session_id=session.session_id,
            device_id=user.device_id,
            ip_address=session.ip_address,
            city=session.city,
            user_type=user.user_type.value,
            page="/",
            referrer=random.choice(["google.com", "facebook.com", "direct", "tiktok.com"]),
        )

    def _gen_browsing_event(self, user: User, session: Session) -> ClickstreamEvent:
        if random.random() < self.config.search_after_pageview:
            session.events_count += 1
            query = random.choice(user.preferred_categories) if user.preferred_categories else "electronics"
            return ClickstreamEvent(
                event_id=random_id("evt_"),
                event_type="search",
                event_time=now_iso(),
                user_id=user.user_id,
                session_id=session.session_id,
                device_id=user.device_id,
                ip_address=session.ip_address,
                city=session.city,
                user_type=user.user_type.value,
                query=query,
                results_count=random.randint(5, 50),
            )
        else:
            return self._gen_product_view(user, session)

    def _gen_product_view(self, user: User, session: Session) -> ClickstreamEvent:
        category = random.choice(user.preferred_categories) if user.preferred_categories else None
        product = self.catalog.get_random_product(category=category)
        session.stage = SessionStage.PRODUCT_VIEWING
        session.events_count += 1
        session._current_product = product
        return ClickstreamEvent(
            event_id=random_id("evt_"),
            event_type="product_view",
            event_time=now_iso(),
            user_id=user.user_id,
            session_id=session.session_id,
            device_id=user.device_id,
            ip_address=session.ip_address,
            city=session.city,
            user_type=user.user_type.value,
            product_id=product.product_id,
            product_name=product.name,
            category=product.category,
            price=product.price,
        )

    def _gen_product_action(self, user: User, session: Session) -> ClickstreamEvent:
        if random.random() < self.config.add_to_cart_rate:
            product = getattr(session, "_current_product", None) or self.catalog.get_random_product()
            session.cart_items.append(product)
            session.cart_total += product.price
            session.stage = SessionStage.CARTING
            session.events_count += 1
            return ClickstreamEvent(
                event_id=random_id("evt_"),
                event_type="add_to_cart",
                event_time=now_iso(),
                user_id=user.user_id,
                session_id=session.session_id,
                device_id=user.device_id,
                ip_address=session.ip_address,
                city=session.city,
                user_type=user.user_type.value,
                product_id=product.product_id,
                product_name=product.name,
                price=product.price,
                quantity=1,
                cart_total=session.cart_total,
            )
        else:
            session.stage = SessionStage.BROWSING
            return self._gen_product_view(user, session)

    def _gen_cart_action(self, user: User, session: Session) -> Optional[ClickstreamEvent]:
        # Ở phễu mới, checkout cuối cùng chuyển sang TransactionEvent!
        session.stage = SessionStage.COMPLETED
        return None

