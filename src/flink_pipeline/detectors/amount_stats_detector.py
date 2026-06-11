import json
import math
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from src.domain.alarm import Alarm

# Ile ostatnich kwot karty trzymamy w oknie
WINDOW_SIZE = 30
# Ile kwot w oknie potrzeba, zanim zaczniemy oceniać
MIN_SAMPLES = 12
# Ile odchyleń standardowych od średniej traktujemy jako anomalię
Z_SCORE_THRESHOLD = 4.0


# z-score kwoty z okna ostatnich WINDOW_SIZE transakcji karty (okno zapomina stare dane)
class AmountStatsAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.window_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("amount_window_state", Types.STRING())
        self.window_state = runtime_context.get_state(descriptor)

    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context'):
        tx = json.loads(value)
        amount = float(tx.get('amount', 0.0))

        state_str = self.window_state.value()
        window = json.loads(state_str) if state_str else []

        # Ocena przed dodaniem bieżącej kwoty - porównujemy ją do ostatnich N
        if len(window) >= MIN_SAMPLES:
            mean = sum(window) / len(window)
            variance = sum((x - mean) ** 2 for x in window) / (len(window) - 1)
            std = math.sqrt(variance)
            if std > 0:
                z = (amount - mean) / std
                if abs(z) > Z_SCORE_THRESHOLD:
                    alarm = Alarm(
                        alarm_type='AMOUNT_ZSCORE_ANOMALY',
                        card_id=tx.get('card_id'),
                        timestamp_iso=tx.get('timestamp'),
                        details={
                            'amount': round(amount, 2),
                            'window_mean': round(mean, 2),
                            'window_std': round(std, 2),
                            'z_score': round(z, 2),
                            'window_size': len(window),
                        }
                    )
                    yield json.dumps(alarm.to_dict())

        # Aktualizacja okna: dodaj bieżącą kwotę i utnij do WINDOW_SIZE ostatnich
        window.append(amount)
        if len(window) > WINDOW_SIZE:
            window = window[-WINDOW_SIZE:]
        self.window_state.update(json.dumps(window))
