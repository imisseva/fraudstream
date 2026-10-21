import random
import uuid

def random_ip(subnet_prefix: str) -> str:
    """Tạo địa chỉ IPv4 ngẫu nhiên từ dải subnet (vd 103.12.44.0 -> 103.12.44.187)."""
    parts = subnet_prefix.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.{parts[2]}.{random.randint(2, 254)}"
    return f"103.12.44.{random.randint(2, 254)}"

def random_id(prefix: str = "id_") -> str:
    """Tạo ID duy nhất với prefix."""
    return f"{prefix}{uuid.uuid4().hex[:8]}"

def generate_amount(mean: float, std: float, min_val: float = 10000.0) -> float:
    """Sinh số tiền theo phân phối log-normal thực tế, làm tròn đến hàng ngàn."""
    val = random.gauss(mean, std)
    amount = max(min_val, val)
    return float(round(amount, -3))  # Làm tròn đến 1.000 VND

