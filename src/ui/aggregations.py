"""
Potok strumieniowy (streamz) agregujący napływające alarmy do metryk
pokazywanych na dashboardzie.

Zamiast pętli ``for`` z ``.append()`` budujemy potok: każdy alarm jest
wpychany do ``Stream`` przez ``source.emit(alarm)``, a operatory
``map`` / ``filter`` / ``accumulate`` / ``sink`` przeliczają go do metryk.
``accumulate`` pełni rolę "reduce po strumieniu" - utrzymuje bieżący wynik.
"""
from collections import Counter
from streamz import Stream


def _amount_at_risk(alarm: dict) -> float:
    """Kwota, na jaką próbowano oszukać dla danego alarmu.
    Dla przekroczenia limitu jest to attempted_amount, dla anomalii z-score
    amount; alarmy 'impossible travel' nie niosą kwoty (0)."""
    details = alarm.get("details", {})
    value = details.get("attempted_amount")
    if value is None:
        value = details.get("amount")
    return float(value) if value is not None else 0.0


class Aggregates:
    """Bieżący stan agregatów, aktualizowany przez sinki potoku."""

    def __init__(self):
        self.money_at_risk = 0.0
        self.by_type = Counter()
        self.max_zscore = 0.0

    def set_money(self, value: float) -> None:
        self.money_at_risk = value

    def set_by_type(self, counter: Counter) -> None:
        self.by_type = counter

    def set_max_zscore(self, value: float) -> None:
        self.max_zscore = value


def build_stream(agg: Aggregates) -> Stream:
    """Buduje potok zasilający obiekt ``agg``. Zwraca źródło - alarmy
    wpychamy do niego przez ``source.emit(alarm)``."""
    source = Stream()

    # 1. Łączna kwota zagrożona - sumowanie potokiem.
    source.map(_amount_at_risk) \
          .accumulate(lambda acc, x: acc + x, start=0.0) \
          .sink(agg.set_money)

    # 2. Liczba alarmów wg typu - akumulacja Counterem.
    source.map(lambda a: a.get("alarm_type", "UNKNOWN")) \
          .accumulate(lambda c, t: c + Counter([t]), start=Counter()) \
          .sink(agg.set_by_type)

    # 3. Maksymalny |z-score| - tylko z anomalii statystycznych.
    source.filter(lambda a: a.get("alarm_type") == "AMOUNT_ZSCORE_ANOMALY") \
          .map(lambda a: abs(float(a.get("details", {}).get("z_score", 0.0)))) \
          .accumulate(lambda m, z: max(m, z), start=0.0) \
          .sink(agg.set_max_zscore)

    return source
