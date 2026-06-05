from dataclasses import dataclass
from src.domain.location import Location

@dataclass
class Card:
    id: str
    user_id: str
    limit: float
    home_location: Location