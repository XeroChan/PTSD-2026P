# Potok streamz agregujący alarmy do metryk dashboardu
from collections import Counter
from streamz import Stream


# Kwota próby oszustwa: attempted_amount (limit) lub amount (z-score)
def _amount_at_risk(alarm: dict) -> float:
    details = alarm.get("details", {})
    value = details.get("attempted_amount")
    if value is None:
        value = details.get("amount")
    return float(value) if value is not None else 0.0


# Bieżący stan agregatów, aktualizowany przez sinki potoku
class Aggregates:
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


# Buduje potok zasilający agg alarmy wpychamy przez source.emit(alarm)
def build_stream(agg: Aggregates) -> Stream:
    source = Stream()

    # Łączna kwota zagrożona - sumowanie potokiem.
    source.map(_amount_at_risk) \
          .accumulate(lambda acc, x: acc + x, start=0.0) \
          .sink(agg.set_money)

    # Liczba alarmów wg typu - akumulacja Counterem.
    source.map(lambda a: a.get("alarm_type", "UNKNOWN")) \
          .accumulate(lambda c, t: c + Counter([t]), start=Counter()) \
          .sink(agg.set_by_type)

    # Maksymalny |z-score| - tylko z anomalii statystycznych.
    source.filter(lambda a: a.get("alarm_type") == "AMOUNT_ZSCORE_ANOMALY") \
          .map(lambda a: abs(float(a.get("details", {}).get("z_score", 0.0)))) \
          .accumulate(lambda m, z: max(m, z), start=0.0) \
          .sink(agg.set_max_zscore)

    return source
