import json
from pathlib import Path
from typing import List, Dict, Any
from simulator.models.user import User, UserType
from simulator.config import SimulatorConfig

class UserGenerator:
    """Tạo và quản lý User Pool gồm người dùng bình thường, đáng ngờ và gian lận."""

    def __init__(self, config: SimulatorConfig, countries: List[Dict[str, Any]], cities: List[Dict[str, str]], devices: List[Dict[str, Any]]):
        self.config = config
        self.countries = countries
        self.cities = cities
        self.devices = devices

    def generate_user_pool(self) -> List[User]:
        users: List[User] = []
        n = self.config.total_users
        
        num_normal = int(n * self.config.normal_user_ratio)
        num_suspicious = int(n * self.config.suspicious_user_ratio)
        num_fraud = int(n * self.config.fraudster_ratio)

        for _ in range(num_normal):
            users.append(User.create_normal(self.countries, self.cities, self.devices))
        for _ in range(num_suspicious):
            users.append(User.create_suspicious(self.countries, self.cities, self.devices))
        for _ in range(num_fraud):
            users.append(User.create_fraudster(self.countries, self.cities, self.devices))

        return users

