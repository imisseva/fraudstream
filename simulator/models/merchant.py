import json
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Union
import random

@dataclass
class Merchant:
    merchant_id: str
    name: str
    category: str        # electronics, jewelry, travel, gaming, supermarket, gas_station, crypto, fashion, online_retail
    risk_level: str      # low, medium, high
    country: str         # VN, US, SG, JP...

class MerchantCatalog:
    def __init__(self, data_path: Optional[Union[str, Path]] = None):
        if data_path is None:
            data_path = Path(__file__).parent.parent / "data" / "merchants.json"
        with open(data_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.merchants: List[Merchant] = [Merchant(**m) for m in raw]
        self.by_category = {}
        for m in self.merchants:
            self.by_category.setdefault(m.category, []).append(m)
        self.high_risk = [m for m in self.merchants if m.risk_level == "high"]

    def get_random_merchant(self, category: Optional[str] = None, country: Optional[str] = None) -> Merchant:
        pool = self.merchants
        if category and category in self.by_category:
            pool = self.by_category[category]
        if country:
            country_pool = [m for m in pool if m.country == country]
            if country_pool:
                pool = country_pool
        return random.choice(pool)

    def get_high_risk_merchant(self) -> Merchant:
        if self.high_risk:
            return random.choice(self.high_risk)
        return random.choice(self.merchants)

