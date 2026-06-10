import time
import random
from src.config.settings import KAFKA_BROKER, TOPIC_RAW
from src.simulator.card_generator import CardGenerator
from src.simulator.transaction_factory import TransactionFactory
from src.infrastructure.kafka_publisher import KafkaPublisher
from src.domain.transaction import Transaction

class SimulatorRunner:
    def __init__(self):
        self.generator = CardGenerator(number_of_cards=10000, number_of_users=5000)
        self.factory = TransactionFactory()
        self.publisher = KafkaPublisher(bootstrap_servers=KAFKA_BROKER)

    def _generate_random_transaction(self, card) -> Transaction:
        chance = random.random()
        
        if chance < 0.01:
            print(f"Anomaly [LOCATION] generated for {card.id}")
            return self.factory.create_location_anomaly(card)

        if chance < 0.02:
            print(f"Anomaly [AMOUNT > LIMIT] generated for {card.id}")
            return self.factory.create_amount_anomaly(card)

        if chance < 0.03:
            print(f"Anomaly [AMOUNT SPIKE] generated for {card.id}")
            return self.factory.create_amount_spike_anomaly(card)

        return self.factory.create_normal_transaction(card)

    def run(self) -> None:
        print("Starting transaction stream...")
        try:
            while True:
                card = self.generator.get_random_card()
                
                # Czasami generuj serię transakcji dla tej samej karty (High Frequency Anomaly)
                if random.random() < 0.005:
                    print(f"Anomaly [HIGH FREQUENCY] burst generated for {card.id}")
                    for _ in range(5):
                        transaction = self.factory.create_normal_transaction(card)
                        transaction.is_anomaly = True
                        transaction.anomaly_type = "HIGH FREQUENCY"
                        self.publisher.publish(
                            topic=TOPIC_RAW,
                            key=transaction.card_id,
                            message=transaction.to_dict()
                        )
                else:
                    transaction = self._generate_random_transaction(card)
                    self.publisher.publish(
                        topic=TOPIC_RAW,
                        key=transaction.card_id,
                        message=transaction.to_dict()
                    )
                
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("\nSimulator stopped.")
        finally:
            self.publisher.flush()

if __name__ == "__main__":
    runner = SimulatorRunner()
    runner.run()