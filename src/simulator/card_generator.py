import random
from typing import List
from faker import Faker
from src.domain.location import Location
from src.domain.card import Card

# Część kart jest używana znacznie częściej
# rozkład użycia kart jest mocno nierównomierny i sprawia, że detektor
# statystyczny zdąży zebrać historię transakcji potrzebną do oceny z-score.
HOT_CARDS_COUNT = 100
HOT_CARDS_PROBABILITY = 0.9

class CardGenerator:
    def __init__(self, number_of_cards: int = 10000, number_of_users: int = 5000):
        self.faker = Faker()
        self.users = [self.faker.uuid4() for _ in range(number_of_users)]
        self.cards: List[Card] = self._generate_cards(number_of_cards)
        self.hot_cards: List[Card] = self.cards[:HOT_CARDS_COUNT]

    def _generate_cards(self, count: int) -> List[Card]:
        print(f"Generating {count} cards...")
        generated = []
        for i in range(count):
            random_user_id = random.choice(self.users)
            
            card = Card(
                id=f"CARD_{i}",
                user_id=random_user_id,
                limit=round(random.uniform(1000, 20000), 2),
                home_location=Location(
                    latitude=float(self.faker.latitude()),
                    longitude=float(self.faker.longitude())
                )
            )
            generated.append(card)
        return generated

    def get_random_card(self) -> Card:
        if random.random() < HOT_CARDS_PROBABILITY:
            return random.choice(self.hot_cards)
        return random.choice(self.cards)