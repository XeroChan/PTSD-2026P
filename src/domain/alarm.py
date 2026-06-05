from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class Alarm:
    alarm_type: str
    card_id: str
    timestamp_iso: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "alarm_type": self.alarm_type,
            "card_id": self.card_id,
            "timestamp": self.timestamp_iso,
            "details": self.details
        }