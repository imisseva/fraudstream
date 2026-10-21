import random
from typing import List, Dict, Any, Optional
from simulator.models.user import User, UserType
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.scenarios.normal import NormalScenario
from simulator.scenarios.high_value import HighValueScenario
from simulator.scenarios.velocity import VelocityScenario
from simulator.scenarios.geographic_anomaly import GeographicAnomalyScenario
from simulator.scenarios.device_ip_sharing import DeviceIpSharingScenario
from simulator.scenarios.repeated_attempts import RepeatedAttemptsScenario
from simulator.generators.data_quality_generator import DataQualityGenerator
from simulator.config import SimulatorConfig

class TransactionGenerator:
    """Điều phối và sinh các sự kiện giao dịch tài chính (Normal + Scenarios A -> E + Data Quality)."""

    def __init__(
        self,
        config: SimulatorConfig,
        merchant_catalog: MerchantCatalog,
        countries: List[Dict[str, Any]],
        dq_generator: Optional[DataQualityGenerator] = None,
    ):
        self.config = config
        self.catalog = merchant_catalog
        self.countries = countries
        self.dq_gen = dq_generator or DataQualityGenerator(config.data_quality_error_rate)

        # Scenarios
        self.normal_scenario = NormalScenario(self.catalog)
        self.scen_high_value = HighValueScenario(self.catalog)
        self.scen_velocity = VelocityScenario(self.catalog)
        self.scen_geo_anomaly = GeographicAnomalyScenario(self.catalog, self.countries)
        self.scen_device_sharing = DeviceIpSharingScenario(self.catalog)
        self.scen_repeated_attempts = RepeatedAttemptsScenario(self.catalog)

        # Queue lưu trữ chuỗi sự kiện sinh theo lô (burst)
        self.event_queue: List[TransactionEvent] = []

    def generate_next_event(self, user: User) -> Dict[str, Any]:
        """Sinh 1 transaction event kế tiếp (dưới dạng Dict), có thể kèm data quality glitch."""
        # 1. Nếu đang có hàng đợi burst (từ velocity hoặc repeated_attempts)
        if self.event_queue:
            raw_event = self.event_queue.pop(0)
        else:
            raw_event = self._generate_scenario_event(user)

        event_dict = raw_event.to_dict()

        # 2. Ghi nhận vào cache để phục vụ kịch bản duplicate
        self.dq_gen.record_event(event_dict)

        # 3. Kiểm tra xem có cần inject data quality error không
        if self.dq_gen.should_inject():
            event_dict = self.dq_gen.inject_glitch(event_dict)

        return event_dict

    def _generate_scenario_event(self, user: User) -> TransactionEvent:
        # Xác định xem giao dịch này có phải là fraud không
        is_fraud = False
        if user.user_type == UserType.FRAUDSTER:
            is_fraud = random.random() < 0.70  # Fraudster có xác suất gian lận cao
        elif user.user_type == UserType.SUSPICIOUS:
            is_fraud = random.random() < 0.25  # Suspicious thỉnh thoảng gian lận
        else:
            is_fraud = random.random() < self.config.fraud_rate  # Normal user tỷ lệ rất thấp (bị hack)

        if not is_fraud:
            return self.normal_scenario.generate(user)

        # Chọn 1 trong 5 Fraud Scenarios theo trọng số
        scenarios = ["A", "B", "C", "D", "E"]
        weights = [
            self.config.scenario_a_weight,
            self.config.scenario_b_weight,
            self.config.scenario_c_weight,
            self.config.scenario_d_weight,
            self.config.scenario_e_weight,
        ]
        chosen = random.choices(scenarios, weights=weights, k=1)[0]

        if chosen == "A":
            return self.scen_high_value.generate(user)
        elif chosen == "B":
            burst = self.scen_velocity.generate_burst(user, burst_count=random.randint(8, 12))
            self.event_queue.extend(burst[1:])
            return burst[0]
        elif chosen == "C":
            return self.scen_geo_anomaly.generate(user)
        elif chosen == "D":
            return self.scen_device_sharing.generate(user)
        else:  # "E"
            seq = self.scen_repeated_attempts.generate_sequence(user, failure_count=random.randint(3, 5))
            self.event_queue.extend(seq[1:])
            return seq[0]

