import json
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from src.domain.alarm import Alarm
from src.utils.geo_utils import calculate_speed_kmh, haversine_distance

MAX_SPEED_KMH = 1000.0
KNOWN_LOCATION_RADIUS_KM = 50.0   # promień, w którym transakcje to ta sama "lokalizacja"
MIN_VISITS_FREQUENT = 3           # ile wizyt, by lokalizacja stała się "częsta"
MAX_KNOWN_LOCATIONS = 10          # limit zapamiętanych lokalizacji per karta


class LocationAnomalyDetector(KeyedProcessFunction):
    """
    Dwa niezależne sygnały lokalizacyjne dla karty, oparte na pamięci stanu Flinka:

    1. IMPOSSIBLE_TRAVEL - czysto prędkościowy. Jeśli prędkość między poprzednią
       a bieżącą transakcją przekracza MAX_SPEED_KMH, to fizyczna niemożliwość -
       alarm leci ZAWSZE, niezależnie od tego, czy miejsce jest znane.

    2. UNUSUAL_LOCATION - korzysta z pamięci CZĘSTYCH lokalizacji karty. Jeśli karta
       ma już ustalony "obszar bywania" (lokalizacje odwiedzone >= MIN_VISITS_FREQUENT
       razy), a bieżąca transakcja jest daleko od wszystkich z nich - alarm.
       Lokalizacja przestaje być nietypowa dopiero, gdy stanie się częsta.
    """

    def __init__(self):
        self.last_tx_state = None
        self.known_locations_state = None

    def open(self, runtime_context: RuntimeContext):
        self.last_tx_state = runtime_context.get_state(
            ValueStateDescriptor("last_transaction_state", Types.STRING()))
        self.known_locations_state = runtime_context.get_state(
            ValueStateDescriptor("known_locations_state", Types.STRING()))

    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context'):
        current_tx = json.loads(value)
        current_loc = current_tx['location']
        card_id = current_tx.get('card_id')

        last_tx_str = self.last_tx_state.value()
        known_str = self.known_locations_state.value()
        known = json.loads(known_str) if known_str else []

        # 1. IMPOSSIBLE TRAVEL - wyłącznie prędkość
        if last_tx_str:
            last_tx = json.loads(last_tx_str)
            speed = calculate_speed_kmh(
                time1_iso=last_tx['timestamp'],
                lat1=last_tx['location']['lat'],
                lon1=last_tx['location']['lon'],
                time2_iso=current_tx['timestamp'],
                lat2=current_loc['lat'],
                lon2=current_loc['lon'],
            )
            if speed > MAX_SPEED_KMH:
                alarm = Alarm(
                    alarm_type='IMPOSSIBLE_TRAVEL',
                    card_id=card_id,
                    timestamp_iso=current_tx['timestamp'],
                    details={
                        'speed_kmh': round(speed, 2),
                        'previous_location': last_tx['location'],
                        'current_location': current_loc,
                    },
                )
                yield json.dumps(alarm.to_dict())

        # 2. UNUSUAL LOCATION - daleko od wszystkich CZĘSTYCH lokalizacji karty
        frequent = [loc for loc in known if loc['count'] >= MIN_VISITS_FREQUENT]
        if frequent:
            near_frequent = any(
                haversine_distance(current_loc['lat'], current_loc['lon'], loc['lat'], loc['lon'])
                <= KNOWN_LOCATION_RADIUS_KM
                for loc in frequent
            )
            if not near_frequent:
                alarm = Alarm(
                    alarm_type='UNUSUAL_LOCATION',
                    card_id=card_id,
                    timestamp_iso=current_tx['timestamp'],
                    details={
                        'current_location': current_loc,
                        'frequent_locations': len(frequent),
                        'note': 'Far from all frequent locations of this card',
                    },
                )
                yield json.dumps(alarm.to_dict())

        # 3. Aktualizacja pamięci częstych lokalizacji (zliczanie wizyt)
        matched = None
        for loc in known:
            if haversine_distance(current_loc['lat'], current_loc['lon'], loc['lat'], loc['lon']) <= KNOWN_LOCATION_RADIUS_KM:
                matched = loc
                break
        if matched:
            matched['count'] += 1
        else:
            known.append({'lat': current_loc['lat'], 'lon': current_loc['lon'], 'count': 1})
            if len(known) > MAX_KNOWN_LOCATIONS:
                known.sort(key=lambda loc: loc['count'])
                known = known[1:]   # usuń najrzadziej odwiedzaną
        self.known_locations_state.update(json.dumps(known))

        # 4. Aktualizacja ostatniej transakcji
        self.last_tx_state.update(json.dumps(current_tx))
