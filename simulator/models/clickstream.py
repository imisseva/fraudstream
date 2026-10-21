from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class ClickstreamEvent:
    event_id: str
    event_type: str                  # page_view, search, product_view, add_to_cart
    event_time: str
    user_id: str
    session_id: str
    device_id: str
    ip_address: str
    city: str
    user_type: str = "normal"
    page: Optional[str] = None
    query: Optional[str] = None
    results_count: Optional[int] = None
    product_id: Optional[str] = None
    product_name: Optional[str] = None
    category: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    cart_total: Optional[float] = None
    referrer: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Bỏ qua các field None để payload JSON sạch sẽ
        return {k: v for k, v in d.items() if v is not None}

