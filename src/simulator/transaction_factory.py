import random
from datetime import datetime, timezone
from src.domain.card import Card
from src.domain.location import Location
from src.domain.transaction import Transaction

class TransactionFactory:
    # Zwykła transakcja blisko domu karty
    def create_normal_transaction(self, card: Card) -> Transaction:
        lat = card.home_location.latitude + random.uniform(-0.1, 0.1)
        lon = card.home_location.longitude + random.uniform(-0.1, 0.1)
        
        amount = round(random.uniform(5, card.limit * 0.2), 2)
        
        return self._build_transaction(card, Location(lat, lon), amount, is_anomaly=False, anomaly_type="NONE")

    # Skok GPS na drugi koniec świata
    def create_location_anomaly(self, card: Card) -> Transaction:
        lat = -card.home_location.latitude if card.home_location.latitude != 0 else 50.0
        lon = card.home_location.longitude + 90.0
        if lon > 180:
            lon -= 360
            
        amount = round(random.uniform(5, card.limit * 0.2), 2)
        return self._build_transaction(card, Location(lat, lon), amount, is_anomaly=True, anomaly_type="LOCATION")

    # Kwota powyżej limitu karty
    def create_amount_anomaly(self, card: Card) -> Transaction:
        lat = card.home_location.latitude + random.uniform(-0.1, 0.1)
        lon = card.home_location.longitude + random.uniform(-0.1, 0.1)

        amount = round(card.limit * random.uniform(1.1, 2.0), 2)
        return self._build_transaction(card, Location(lat, lon), amount, is_anomaly=True, anomaly_type="AMOUNT > LIMIT")

    # Skok kwoty wciąż poniżej limitu - łapie go detektor z-score
    def create_amount_spike_anomaly(self, card: Card) -> Transaction:
        lat = card.home_location.latitude + random.uniform(-0.1, 0.1)
        lon = card.home_location.longitude + random.uniform(-0.1, 0.1)

        amount = round(card.limit * random.uniform(0.5, 0.95), 2)
        return self._build_transaction(card, Location(lat, lon), amount, is_anomaly=True, anomaly_type="AMOUNT SPIKE")

    def _build_transaction(self, card: Card, location: Location, amount: float, is_anomaly: bool = False, anomaly_type: str = "NONE") -> Transaction:
        return Transaction(
            card_id=card.id,
            user_id=card.user_id,
            location=location,
            amount=amount,
            available_limit=card.limit,
            timestamp_iso=datetime.now(timezone.utc).isoformat(),
            is_anomaly=is_anomaly,
            anomaly_type=anomaly_type
        )