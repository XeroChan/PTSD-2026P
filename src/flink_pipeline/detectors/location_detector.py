import json
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from src.domain.alarm import Alarm
from src.utils.geo_utils import calculate_speed_kmh, haversine_distance

MAX_SPEED_KMH = 1000.0
MAX_KNOWN_LOCATIONS = 5
KNOWN_LOCATION_THRESHOLD_KM = 50.0

class LocationAnomalyDetector(KeyedProcessFunction):
    """
    Wykrywa transakcje "Niemożliwej podróży" (Impossible Travel).
    Wykorzystuje mechanizm State we Flinku, by zapamiętać poprzednią lokalizację karty
    oraz historię częstych/znanych lokalizacji w celu wykluczenia fałszywych alarmów.
    """
    
    def __init__(self):
        self.last_tx_state = None
        self.known_locations_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor_last_tx = ValueStateDescriptor("last_transaction_state", Types.STRING())
        self.last_tx_state = runtime_context.get_state(descriptor_last_tx)
        
        descriptor_known_locs = ValueStateDescriptor("known_locations_state", Types.STRING())
        self.known_locations_state = runtime_context.get_state(descriptor_known_locs)

    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context'):
        current_tx = json.loads(value)
        current_loc = current_tx['location']
        
        last_tx_str = self.last_tx_state.value() if self.last_tx_state is not None else None
        
        # 1. Odczytaj znane lokalizacje
        known_locations_str = self.known_locations_state.value() if self.known_locations_state is not None else None
        if known_locations_str:
            known_locations = json.loads(known_locations_str)
        else:
            known_locations = []

        is_known_location = False
        for loc in known_locations:
            dist = haversine_distance(current_loc['lat'], current_loc['lon'], loc['lat'], loc['lon'])
            if dist <= KNOWN_LOCATION_THRESHOLD_KM:
                is_known_location = True
                break

        # 2. Wykrywanie Impossible Travel
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
            
            # Jeśli prędkość jest za duża, ale lokalizacja JEST znana - ignorujemy alarm
            if speed > MAX_SPEED_KMH and not is_known_location:
                alarm = Alarm(
                    alarm_type='IMPOSSIBLE_TRAVEL',
                    card_id=current_tx['card_id'],
                    timestamp_iso=current_tx['timestamp'],
                    details={
                        'speed_kmh': round(speed, 2),
                        'previous_location': last_tx['location'],
                        'current_location': current_loc,
                        'note': 'Location not in history'
                    }
                )
                yield json.dumps(alarm.to_dict())

        # 3. Aktualizacja stanu
        if self.last_tx_state is not None:
            self.last_tx_state.update(json.dumps(current_tx))
            
        if self.known_locations_state is not None and not is_known_location:
            known_locations.append(current_loc)
            # Trzymajmy w historii tylko X ostatnich nowo poznanych lokalizacji
            if len(known_locations) > MAX_KNOWN_LOCATIONS:
                known_locations.pop(0) 
            self.known_locations_state.update(json.dumps(known_locations))