import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer

from config import KAFKA_BROKER, TOPIC_RAW, TOPIC_ALARMS, GROUP_ID
from detectors import TravelAnomalyDetector

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Dodajemy zależności do klastra Flinka, aby workery miały dostęp do modułów
    env.add_python_file("config.py")
    env.add_python_file("geo_utils.py")
    env.add_python_file("detectors.py")
    
    # 1. Źródło (Czytanie z Kafki)
    kafka_consumer = FlinkKafkaConsumer(
        topics=TOPIC_RAW,
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': KAFKA_BROKER, 'group.id': GROUP_ID}
    )
    
    # 3. Ujście (Zapis alarmów do drugiego tematu Kafki)
    kafka_producer = FlinkKafkaProducer(
        topic=TOPIC_ALARMS,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': KAFKA_BROKER}
    )
    
    # 2. Przetwarzanie flink - zbudowanie uproszczonego pipeline'u
    env.add_source(kafka_consumer) \
       .key_by(lambda x: json.loads(x)['card_id']) \
       .process(TravelAnomalyDetector()) \
       .add_sink(kafka_producer)

    # Uruchomienie zadania na JobManagerze
    env.execute("Anomaly Detection Job")

if __name__ == '__main__':
    main()
