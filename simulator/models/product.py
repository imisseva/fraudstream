import json
import random
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional, Union

@dataclass
class Product:
    product_id: str
    name: str
    category: str
    price: float           # VND
    is_high_risk: bool     # Gift cards, electronics đắt tiền

class ProductCatalog:
    def __init__(self, data_path: Optional[Union[str, Path]] = None):
        if data_path is None:
            data_path = Path(__file__).parent.parent / "data" / "products.json"
        
        with open(data_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        
        self.products: List[Product] = [Product(**p) for p in raw]
        self.by_category: Dict[str, List[Product]] = {}
        for p in self.products:
            self.by_category.setdefault(p.category, []).append(p)
        self.high_risk: List[Product] = [p for p in self.products if p.is_high_risk]
    
    def get_random_product(self, category: Optional[str] = None) -> Product:
        if category and category in self.by_category:
            return random.choice(self.by_category[category])
        return random.choice(self.products)
    
    def get_high_risk_product(self) -> Product:
        if not self.high_risk:
            return random.choice(self.products)
        return random.choice(self.high_risk)

    def get_categories(self) -> List[str]:
        return list(self.by_category.keys())

