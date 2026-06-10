# Priorytety

## Must-have

- [x] Domknąć generator transakcji tak, żeby spełniał wymagania danych: 10 000 kart, wielu kart na jednego użytkownika, JSON, kilka typów anomalii. (`src/simulator/` — card_generator, transaction_factory, runner)
- [x] Przerobić job Flinka na realny detektor (a nie prototyp), liczący metryki i wykrywający anomalie statystycznie. (`src/flink_pipeline/detectors/`)
- [x] Ujednolicić konfigurację Kafki dla hosta i kontenerów przez zmienne środowiskowe (`KAFKA_BROKER`). (`src/config/settings.py`, `docker-compose.yaml`)
- [x] Dodać osobnego testowego konsumenta dla `raw_transactions` (komponent c ze spec): walidacja generowanych danych — liczba unikalnych kart (10 000), rozkład kwot/lokalizacji, podział normalne vs anomalie — z wizualizacją. Dashboard pokazuje dziś tylko `alarms`.

## Should-have

- [x] Pamięć częstych lokalizacji w detektorze. (`location_detector.py` — zliczanie wizyt + alarm UNUSUAL_LOCATION)
- [x] Podłączyć MongoDB do realnego zapisu alarmów. (`src/persistence/alarm_store.py` + `src/infrastructure/mongo_repository.py`, usługa `mongo` w compose) + odczyt historii na dashboardzie.
- [x] Dodać co najmniej jeszcze jedną anomalię poza skokiem kwoty. Mamy: niemożliwa podróż (prędkość), nietypowa lokalizacja, skok kwoty pod limitem, przekroczenie limitu, anomalia statystyczna z-score.

## Nice-to-have

- [x] Rozbudować dashboard o lepszą wizualizację. (`src/ui/dashboard.py` + `aggregations.py`: agregaty streamz — kwota zagrożona, alarmy wg typu, max z-score; tabele per typ; pełna historia z MongoDB)
- [ ] Automatyczne zgłaszanie joba Flink przy starcie (teraz ręcznie: `flink run -d -py /app/src/flink_pipeline/job.py`).
<!-- TODO: Automatyczne zgłaszanie joba Flink przy starcie-->
- [ ] Automatyczne tworzenie topiców Kafki przy starcie; przy okazji `raw_transactions` z kilkoma partycjami, żeby zrównoleglić też odczyt ze źródła.
<!-- TODO: Automatyczne tworzenie topiców przy starcie. -->
- [ ] Dodać anomalię częstotliwości transakcji (zbyt wiele transakcji w krótkim czasie) jako kolejny typ.
<!-- TODO: Dodanie anomalii częstotliwości transakcji. -->
- [ ] Dokumentacja uruchomienia (README): host vs kontenery, porty (8501 dashboard, 8081 Flink, 9092 Kafka, 27017 Mongo), co startuje w Dockerze, jak zgłosić job, jak podejrzeć/wyczyścić Mongo.
<!-- TODO: Dodać dokumentację uruchomienia. -->
- [ ] (opcjonalnie) Bufor per typ dla tabel live na dashboardzie, żeby burst jednego typu nie wypychał innych z okna 100.
<!-- TODO: Bufor per typ dla tabel live na dashboardzie. -->
- [ ] (opcjonalnie) Usunąć przestarzałe `version: '3.8'` z `docker-compose.yaml`.
<!-- TODO: Usunąć przestarzałe `version: '3.8'` z `docker-compose.yaml`. -->
