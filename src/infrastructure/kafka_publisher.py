import json
from typing import Any, Dict
from confluent_kafka import Producer

class KafkaPublisher:
    def __init__(self, bootstrap_servers: str):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def publish(self, topic: str, key: str, message: Dict[str, Any]) -> None:
        try:
            json_data = json.dumps(message)
            self.producer.produce(
                topic,
                key=key,
                value=json_data,
                callback=self._delivery_report
            )
            # poll(0) wyzwala callbacki dostarczenia bez blokowania
            self.producer.poll(0)
        except Exception as e:
            print(f"Błąd wysyłania do Kafki: {e}")

    def flush(self) -> None:
        self.producer.flush()

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            print(f"Błąd dostarczenia: {err}")