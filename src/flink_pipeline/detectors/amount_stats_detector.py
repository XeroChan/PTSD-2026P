import json
import math
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from src.domain.alarm import Alarm

# Ile transakcji karty musimy zaobserwować, zanim zaczniemy oceniać (rozgrzewka).
MIN_SAMPLES = 12
# Ile odchyleń standardowych od średniej traktujemy jako anomalię.
Z_SCORE_THRESHOLD = 4.0


class AmountStatsAnomalyDetector(KeyedProcessFunction):
    """
    Wykrywa nagłą zmianę wartości transakcji metodą statystyczną (z-score).

    Dla każdej karty utrzymuje w pamięci Flinka (state) bieżącą liczność,
    średnią oraz sumę kwadratów odchyleń (M2) i aktualizuje je przyrostowo
    algorytmem Welforda - bez przechowywania całej historii kwot.
    """

    def __init__(self):
        self.stats_state = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("amount_stats_state", Types.STRING())
        self.stats_state = runtime_context.get_state(descriptor)

    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context'):
        tx = json.loads(value)
        amount = float(tx.get('amount', 0.0))

        state_str = self.stats_state.value() if self.stats_state is not None else None
        if state_str:
            stats = json.loads(state_str)
        else:
            stats = {"count": 0, "mean": 0.0, "m2": 0.0}

        count = stats["count"]
        mean = stats["mean"]
        m2 = stats["m2"]

        # Ocena PRZED aktualizacją - porównujemy nową kwotę do dotychczasowego rozkładu.
        if count >= MIN_SAMPLES:
            variance = m2 / (count - 1)
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
                            'historical_mean': round(mean, 2),
                            'historical_std': round(std, 2),
                            'z_score': round(z, 2),
                            'samples': count,
                        }
                    )
                    yield json.dumps(alarm.to_dict())

        # Aktualizacja statystyk (Welford) PO ocenie.
        count += 1
        delta = amount - mean
        mean += delta / count
        delta2 = amount - mean
        m2 += delta * delta2

        if self.stats_state is not None:
            self.stats_state.update(json.dumps({"count": count, "mean": mean, "m2": m2}))
