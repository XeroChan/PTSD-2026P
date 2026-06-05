import json
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from src.domain.alarm import Alarm
from src.utils.geo_utils import calculate_speed_kmh

MAX_SPEED_KMH = 1000.0

class LocationAnomalyDetector(KeyedProcessFunction):
    """
    Wykrywa transakcje "Niemożliwej podróży" (Impossible Travel).
    Wykorzystuje mechanizm State we Flinku, by zapamiętać poprzednią lokalizację karty.
    """
    
    def __init__(self):
        self.last_tx_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("last_transaction_state", Types.STRING())
        self.last_tx_state = runtime_context.get_state(descriptor)

    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context'):
        current_tx = json.loads(value)
        
        last_tx_str = self.last_tx_state.value() if self.last_tx_state is not None else None
        
        if last_tx_str:
            last_tx = json.loads(last_tx_str)
            
            speed = calculate_speed_kmh(
                time1_iso=last_tx['timestamp'],
                lat1=last_tx['location']['lat'],
                lon1=last_tx['location']['lon'],
                time2_iso=current_tx['timestamp'],
                lat2=current_tx['location']['lat'],
                lon2=current_tx['location']['lon']
            )
            
            if speed > MAX_SPEED_KMH:
                alarm = Alarm(
                    alarm_type='IMPOSSIBLE_TRAVEL',
                    card_id=current_tx['card_id'],
                    timestamp_iso=current_tx['timestamp'],
                    details={
                        'speed_kmh': round(speed, 2),
                        'previous_location': last_tx['location'],
                        'current_location': current_tx['location']
                    }
                )
                yield json.dumps(alarm.to_dict())

        if self.last_tx_state is not None:
            self.last_tx_state.update(json.dumps(current_tx))