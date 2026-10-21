from datetime import datetime, timezone, timedelta
import random

def now_iso() -> str:
    """Trả về timestamp hiện tại dạng chuẩn ISO 8601 UTC (e.g. 2026-10-21T09:15:30Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def late_timestamp_iso(min_hours_ago: float = 1.0, max_hours_ago: float = 24.0) -> str:
    """Sinh timestamp trễ trong quá khứ để test Spark Watermark / late data."""
    hours = random.uniform(min_hours_ago, max_hours_ago)
    late_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    return late_time.strftime("%Y-%m-%dT%H:%M:%SZ")

def invalid_timestamp() -> str:
    """Sinh chuỗi timestamp bị hỏng / sai định dạng."""
    glitches = [
        "2026-99-99T99:99:99Z",
        "INVALID_DATETIME_VALUE",
        "21/10/2026 15:30",
        "null",
        "0000-00-00T00:00:00Z"
    ]
    return random.choice(glitches)

