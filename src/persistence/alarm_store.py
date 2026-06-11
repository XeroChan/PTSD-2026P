import json
from confluent_kafka import Consumer
from src.config.settings import (
    KAFKA_BROKER,
    TOPIC_ALARMS,
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_ALARMS,
)
from src.infrastructure.mongo_repository import MongoRepository


# Drugi konsument Kafki (obok dashboardu): zapisuje alarmy z topiku do MongoDB
class AlarmStore:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'mongo-alarm-store-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        })
        self.consumer.subscribe([TOPIC_ALARMS])
        self.repository = MongoRepository(
            uri=MONGO_URI,
            db_name=MONGO_DB,
            collection_name=MONGO_COLLECTION_ALARMS,
        )

    def run(self) -> None:
        print(f"Alarm store: czytam '{TOPIC_ALARMS}', zapisuję do MongoDB ({MONGO_URI})...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                try:
                    alarm = json.loads(msg.value().decode('utf-8'))
                    self.repository.save_alarm(alarm)
                    print(f"Zapisano alarm: {alarm.get('alarm_type')} / {alarm.get('card_id')}")
                except Exception as e:
                    print(f"Nie udało się zapisać alarmu: {e}")
        except KeyboardInterrupt:
            print("\nAlarm store zatrzymany.")
        finally:
            self.consumer.close()
            self.repository.close()


if __name__ == "__main__":
    AlarmStore().run()
