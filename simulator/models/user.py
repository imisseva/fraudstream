import uuid
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from simulator.utils.random_utils import random_ip

class UserType(Enum):
    NORMAL = "normal"
    SUSPICIOUS = "suspicious"
    FRAUDSTER = "fraudster"

@dataclass
class Card:
    card_id: str
    user_id: str
    card_type: str = "credit"   # credit, debit
    issuer: str = "Visa"        # Visa, Mastercard, Napas, JCB
    is_active: bool = True
    last_country: Optional[str] = None
    last_ip: Optional[str] = None

@dataclass
class User:
    user_id: str
    user_type: UserType
    cards: List[Card]
    
    # Địa lý & thiết bị thường trú
    home_country: str
    home_city: str
    home_ip: str
    primary_device: Dict[str, Any]
    
    # Baseline hành vi chi tiêu (dùng để phát hiện Scenario A - Unusually large)
    avg_transaction_amount: float
    max_usual_amount: float
    preferred_categories: List[str]
    
    # State tracking
    current_session: Optional[Any] = None
    orders_today: int = 0

    @property
    def primary_card(self) -> Card:
        return self.cards[0] if self.cards else Card(f"card_{uuid.uuid4().hex[:6]}", self.user_id)

    @property
    def device_id(self) -> str:
        return self.primary_device.get("device_id", "dev_001")

    @classmethod
    def create_normal(cls, countries: List[Dict[str, Any]], cities: List[Dict[str, str]], devices: List[Dict[str, Any]]) -> "User":
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        city = random.choice(cities)
        device = random.choice(devices)
        
        # 1-3 thẻ ngân hàng
        num_cards = random.randint(1, 3)
        cards = [
            Card(
                card_id=f"card_{uuid.uuid4().hex[:6]}",
                user_id=user_id,
                card_type=random.choice(["credit", "debit"]),
                issuer=random.choice(["Visa", "Mastercard", "Napas"]),
                last_country="VN",
            )
            for _ in range(num_cards)
        ]

        avg_amt = float(random.choice([200000, 350000, 500000, 800000, 1500000]))
        max_amt = avg_amt * random.uniform(3.0, 5.0)

        return cls(
            user_id=user_id,
            user_type=UserType.NORMAL,
            cards=cards,
            home_country="VN",
            home_city=city["name"],
            home_ip=random_ip(city["ip_range"]),
            primary_device=device,
            avg_transaction_amount=avg_amt,
            max_usual_amount=max_amt,
            preferred_categories=random.sample(
                ["supermarket", "fashion", "online_retail", "electronics", "travel"], k=3
            ),
        )

    @classmethod
    def create_suspicious(cls, countries: List[Dict[str, Any]], cities: List[Dict[str, str]], devices: List[Dict[str, Any]]) -> "User":
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        city = random.choice(cities)
        device = random.choice(devices)
        
        cards = [
            Card(
                card_id=f"card_{uuid.uuid4().hex[:6]}",
                user_id=user_id,
                card_type="credit",
                issuer=random.choice(["Visa", "Mastercard"]),
                last_country="VN",
            )
        ]

        return cls(
            user_id=user_id,
            user_type=UserType.SUSPICIOUS,
            cards=cards,
            home_country="VN",
            home_city=city["name"],
            home_ip=random_ip(city["ip_range"]),
            primary_device=device,
            avg_transaction_amount=1000000.0,
            max_usual_amount=3000000.0,
            preferred_categories=["electronics", "crypto", "jewelry"],
        )

    @classmethod
    def create_fraudster(cls, countries: List[Dict[str, Any]], cities: List[Dict[str, str]], devices: List[Dict[str, Any]]) -> "User":
        user_id = f"fraud_{uuid.uuid4().hex[:8]}"
        city = random.choice(cities)
        device = random.choice(devices)
        
        # Fraudster có thể nắm nhiều thẻ đánh cắp
        num_cards = random.randint(2, 5)
        cards = [
            Card(
                card_id=f"card_{uuid.uuid4().hex[:6]}",
                user_id=user_id,
                card_type="credit",
                issuer=random.choice(["Visa", "Mastercard"]),
                last_country=random.choice(["VN", "US", "SG"]),
            )
            for _ in range(num_cards)
        ]

        return cls(
            user_id=user_id,
            user_type=UserType.FRAUDSTER,
            cards=cards,
            home_country="VN",
            home_city=city["name"],
            home_ip=random_ip(city["ip_range"]),
            primary_device=device,
            avg_transaction_amount=5000000.0,
            max_usual_amount=20000000.0,
            preferred_categories=["jewelry", "crypto", "gaming", "electronics"],
        )

# Alias for compatibility
UserProfile = User
