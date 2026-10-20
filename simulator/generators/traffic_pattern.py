from datetime import datetime
from simulator.config import HOURLY_TRAFFIC

class TrafficPattern:
    """Điều chỉnh số events/giây theo giờ trong ngày"""

    def __init__(self, base_rate: float):
        self.base_rate = float(base_rate)

    def get_current_rate(self) -> float:
        """Trả về số events/giây cho thời điểm hiện tại"""
        hour = datetime.now().hour
        multiplier = HOURLY_TRAFFIC.get(hour, 0.5)
        return self.base_rate * multiplier

    def get_sleep_interval(self) -> float:
        """Trả về thời gian sleep giữa mỗi event (giây)"""
        rate = self.get_current_rate()
        if rate <= 0:
            return 1.0
        return 1.0 / rate

