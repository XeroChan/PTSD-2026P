from dataclasses import dataclass
from src.domain.location import Location

@dataclass
class Transaction:
    card_id: str
    user_id: str
    location: Location
    amount: float
    available_limit: float
    timestamp_iso: str
    is_anomaly: bool = False
    anomaly_type: str = "NONE"

    def to_dict(self) -> dict:
        return {
            "card_id": self.card_id,
            "user_id": self.user_id,
            "location": {
                "lat": self.location.latitude,
                "lon": self.location.longitude
            },
            "amount": self.amount,
            "limit": self.available_limit,
            "timestamp": self.timestamp_iso,
            "is_anomaly": self.is_anomaly,
            "anomaly_type": self.anomaly_type
        }