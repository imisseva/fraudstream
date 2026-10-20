import uuid
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

class UserType(Enum):
    NORMAL = "normal"
    SUSPICIOUS = "suspicious"
    FRAUDSTER = "fraudster"

@dataclass
class User:
    user_id: str
    user_type: UserType
    
    # Profile cố định
    home_city: str           # Thành phố thường trú
    home_ip: str             # IP thường dùng
    primary_device: Dict[str, Any]  # Device chính
    
    # Behavior config (dựa vào user_type)
    avg_session_duration: int       # Giây trung bình 1 session
    avg_time_between_events: float  # Giây giữa mỗi event
    max_orders_per_day: int
    preferred_categories: List[str]
    
    # State (thay đổi runtime)
    current_session: Optional[Any] = None
    orders_today: int = 0
    
    @classmethod
    def _generate_ip(cls, ip_range: str) -> str:
        """Tạo địa chỉ IP ngẫu nhiên từ subnet prefix (vd 103.12.44.0 -> 103.12.44.87)"""
        parts = ip_range.split(".")
        if len(parts) == 4 and parts[3] == "0":
            return f"{parts[0]}.{parts[1]}.{parts[2]}.{random.randint(2, 254)}"
        return ip_range

    @classmethod
    def create_normal(cls, cities: List[Dict[str, str]], devices: List[Dict[str, Any]]) -> "User":
        """Tạo user bình thường"""
        city = random.choice(cities)
        return cls(
            user_id=f"user_{uuid.uuid4().hex[:8]}",
            user_type=UserType.NORMAL,
            home_city=city["name"],
            home_ip=cls._generate_ip(city["ip_range"]),
            primary_device=random.choice(devices),
            avg_session_duration=random.randint(180, 600),   # 3-10 phút
            avg_time_between_events=random.uniform(3.0, 15.0),  # 3-15s
            max_orders_per_day=2,
            preferred_categories=random.sample(
                ["electronics", "fashion", "home", "books", "sports"], k=3
            ),
        )

    @classmethod
    def create_suspicious(cls, cities: List[Dict[str, str]], devices: List[Dict[str, Any]]) -> "User":
        """Tạo user đáng ngờ"""
        city = random.choice(cities)
        return cls(
            user_id=f"user_{uuid.uuid4().hex[:8]}",
            user_type=UserType.SUSPICIOUS,
            home_city=city["name"],
            home_ip=cls._generate_ip(city["ip_range"]),
            primary_device=random.choice(devices),
            avg_session_duration=random.randint(60, 180),    # 1-3 phút
            avg_time_between_events=random.uniform(1.0, 5.0),
            max_orders_per_day=5,
            preferred_categories=["electronics", "gift_cards"],
        )

    @classmethod
    def create_fraudster(cls, cities: List[Dict[str, str]], devices: List[Dict[str, Any]]) -> "User":
        """Tạo fraudster"""
        city = random.choice(cities)
        return cls(
            user_id=f"fraud_{uuid.uuid4().hex[:8]}",
            user_type=UserType.FRAUDSTER,
            home_city=city["name"],
            home_ip=cls._generate_ip(city["ip_range"]),
            primary_device=random.choice(devices),
            avg_session_duration=random.randint(30, 120),    # 30s-2 phút
            avg_time_between_events=random.uniform(0.5, 2.0),  # Rất nhanh
            max_orders_per_day=50,
            preferred_categories=["gift_cards", "electronics"],
        )

