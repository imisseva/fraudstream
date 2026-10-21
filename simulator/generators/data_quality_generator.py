import random
from typing import Dict, Any, List, Optional
from simulator.utils.timestamps import late_timestamp_iso, invalid_timestamp

class DataQualityGenerator:
    """Tự động inject các loại dữ liệu lỗi / bất thường vào events để test pipeline streaming."""

    def __init__(self, error_rate: float = 0.02):
        self.error_rate = error_rate
        self.recent_events_cache: List[Dict[str, Any]] = []
        self.max_cache_size = 50

    def should_inject(self) -> bool:
        return random.random() < self.error_rate

    def inject_glitch(self, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Chọn ngẫu nhiên 1 trong 5 kịch bản lỗi dữ liệu."""
        glitch_type = random.choice([
            "duplicate",
            "missing_field",
            "corrupted_type",
            "late_arriving",
            "invalid_timestamp",
        ])

        corrupted = dict(event_dict)

        if glitch_type == "duplicate" and self.recent_events_cache:
            # Gửi lại nguyên vẹn 1 event đã từng gửi trong quá khứ
            duplicate_event = dict(random.choice(self.recent_events_cache))
            duplicate_event["_data_quality_glitch"] = "duplicate"
            return duplicate_event

        elif glitch_type == "missing_field":
            # Bỏ khuyết một trong các trường quan trọng
            field_to_drop = random.choice(["card_id", "amount", "user_id", "merchant_id"])
            corrupted.pop(field_to_drop, None)
            corrupted["_data_quality_glitch"] = f"missing_{field_to_drop}"
            return corrupted

        elif glitch_type == "corrupted_type":
            # Biến đổi trường amount thành chuỗi chữ hoặc None
            corrupted["amount"] = random.choice(["abc", "NaN", -9999999.0, "12500000_STR"])
            corrupted["_data_quality_glitch"] = "corrupted_amount_type"
            return corrupted

        elif glitch_type == "late_arriving":
            # Timestamp đến trễ 2 đến 24 giờ
            corrupted["event_time"] = late_timestamp_iso(min_hours_ago=2.0, max_hours_ago=24.0)
            corrupted["_data_quality_glitch"] = "late_arriving_event"
            return corrupted

        else:  # invalid_timestamp
            corrupted["event_time"] = invalid_timestamp()
            corrupted["_data_quality_glitch"] = "invalid_timestamp"
            return corrupted

    def record_event(self, event_dict: Dict[str, Any]):
        """Lưu lại event để phục vụ kịch bản sinh duplicate."""
        if len(self.recent_events_cache) >= self.max_cache_size:
            self.recent_events_cache.pop(0)
        self.recent_events_cache.append(event_dict)

