import json
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import SimpleStringSchema

class TravelAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.last_location_state = None

    def open(self, runtime_context: RuntimeContext):
        # INICJALIZACJA PAMIĘCI TYMCZASOWEJ (STATE)
        # Zapisujemy obiekt JSON jako string. Flink sam przypisze ten stan do odpowiedniego card_id
        descriptor = ValueStateDescriptor("last_location", Types.STRING())
        self.last_location_state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current_tx = json.loads(value)
        
        # Pobranie poprzedniej transakcji z pamięci Flinka
        last_tx_str = self.last_location_state.value()
        
        if last_tx_str is not None:
            last_tx = json.loads(last_tx_str)
            
            # Algorytm do wykrywania dystansu?
            # Dystans (np. wzorem Haversine'a na podst. lat/lon)
            # oraz różnicę czasu (między current_tx['timestamp'] a last_tx['timestamp'])
            # pseudo-kod: speed = distance / time_diff
            
            speed = 1500 # Przykładowy wynik z obliczeń (km/h)
            
            if speed > 1000: # Jeśli prędkość > 1000 km/h (szybciej niż samolot)
                alarm = {
                    'alarm_type': 'IMPOSSIBLE_TRAVEL',
                    'card_id': current_tx['card_id'],
                    'speed_kmh': speed,
                    'tx_1': last_tx,
                    'tx_2': current_tx
                }
                # Zwracamy alarm do głównego strumienia
                yield json.dumps(alarm)

        # Nadpisanie pamięci nową transakcją (teraz ta jest "ostatnia")
        self.last_location_state.update(json.dumps(current_tx))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 1. Źródło (Czytanie z Kafki)
    kafka_consumer = FlinkKafkaConsumer(
        topics='raw_transactions',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'kafka:29092', 'group.id': 'flink-group'}
    )
    
    stream = env.add_source(kafka_consumer)

    # 2. Przetwarzanie flink
    alarms_stream = stream \
        .key_by(lambda x: json.loads(x)['card_id']) \
        .process(TravelAnomalyDetector())

    # 3. Ujście (Zapis alarmów do drugiego tematu Kafki)
    kafka_producer = FlinkKafkaProducer(
        topic='alarms',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:29092'}
    )
    
    alarms_stream.add_sink(kafka_producer)

    # Uruchomienie zadania na JobManagerze
    env.execute("Anomaly Detection Job")

if __name__ == '__main__':
    main()