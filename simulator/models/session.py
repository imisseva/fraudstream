import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Optional

class SessionStage(Enum):
    """User đang ở bước nào trong funnel"""
    LANDING = "landing"           # Vừa mở web
    BROWSING = "browsing"         # Đang lướt
    PRODUCT_VIEWING = "viewing"   # Đang xem sản phẩm
    CARTING = "carting"           # Đã add to cart
    CHECKOUT = "checkout"         # Đang thanh toán
    COMPLETED = "completed"       # Đã mua xong
    ABANDONED = "abandoned"       # Bỏ giỏ hàng

@dataclass
class Session:
    session_id: str = field(default_factory=lambda: f"sess_{uuid.uuid4().hex[:12]}")
    stage: SessionStage = SessionStage.LANDING
    events_count: int = 0
    cart_items: List[Any] = field(default_factory=list)
    cart_total: float = 0.0
    
    # Metadata cho fraud detection
    ip_address: str = ""
    device: dict = field(default_factory=dict)
    city: str = ""
    _current_product: Optional[Any] = None
    
    def is_active(self) -> bool:
        return self.stage not in (SessionStage.COMPLETED, SessionStage.ABANDONED)

