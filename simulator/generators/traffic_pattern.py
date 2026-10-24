from datetime import datetime, timezone, timedelta
from simulator.config import HOURLY_TRAFFIC

class TrafficPattern:
    """Điều chỉnh số events/giây theo giờ trong ngày (theo múi giờ Việt Nam UTC+7)."""

    def __init__(self, base_rate: float, tz_offset_hours: int = 7):
        self.base_rate = float(base_rate)
        self.tz = timezone(timedelta(hours=tz_offset_hours))  # Cố định UTC+7 cho dù chạy trên Docker hay Cloud

    def get_current_rate(self) -> float:
        """Trả về số events/giây cho thời điểm hiện tại theo giờ Việt Nam."""
        hour = datetime.now(self.tz).hour
        multiplier = HOURLY_TRAFFIC.get(hour, 0.5)
        return self.base_rate * multiplier

    def get_sleep_interval(self) -> float:
        """Trả về thời gian sleep giữa mỗi event (giây)"""
        rate = self.get_current_rate()
        if rate <= 0:
            return 1.0
        return 1.0 / rate

