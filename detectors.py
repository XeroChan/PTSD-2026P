import json
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from config import MAX_ALLOWED_SPEED_KMH
from geo_utils import calculate_speed_kmh

class TravelAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.last_location_state = None

    def open(self, runtime_context: RuntimeContext):
        # INICJALIZACJA PAMIĘCI TYMCZASOWEJ (STATE)
        descriptor = ValueStateDescriptor("last_location", Types.STRING())
        self.last_location_state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current_tx = json.loads(value)
        
        # Pobranie poprzedniej transakcji z pamięci Flinka
        last_tx_str = self.last_location_state.value()
        
        if last_tx_str is not None:
            last_tx = json.loads(last_tx_str)
            
            # Obliczenie prędkości na podstawie współrzędnych i czasu
            speed = calculate_speed_kmh(last_tx, current_tx)
            
            if speed > MAX_ALLOWED_SPEED_KMH:
                alarm = {
                    'alarm_type': 'IMPOSSIBLE_TRAVEL',
                    'card_id': current_tx.get('card_id'),
                    'speed_kmh': round(speed, 2),
                    'tx_1': last_tx,
                    'tx_2': current_tx
                }
                # Zwracamy alarm do głównego strumienia
                yield json.dumps(alarm)

        # Nadpisanie pamięci nową transakcją (teraz ta jest "ostatnia")
        self.last_location_state.update(json.dumps(current_tx))
