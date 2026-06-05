# Priorytety

## Must-have

- [ ] Domknąć generator transakcji w producer.py tak, żeby spełniał wymagania danych: 10 000 kart, wielu kart na jednego użytkownika, JSON, kilka typów anomalii. 
<!-- TODO: Domknąć generator transakcji dla spełnienia wymagań projektu -->
- [ ] Przerobić flink-job.py na realny detektor, a nie prototyp z `speed = 1500`; trzeba liczyć metryki i wykrywać anomalie statystycznie.
<!-- TODO: Przerobić flink job na realny detektor -->
- [ ] Ujednolicić konfigurację Kafki dla hosta i kontenerów, najlepiej przez zmienne środowiskowe, bo teraz producer.py i app.py łączą się z `localhost:9092`, a flink-job.py z `kafka:29092`.
<!-- TODO: Ujednolicić konfigurację Kafki dla hosta i kontenerów.
Najlepiej przez zmienne środowiskowe -->
- [ ] Dodać osobnego testowego konsumenta dla `raw_transactions`, bo app.py dziś pokazuje tylko `alarms`.
<!-- TODO: Dodać osobnego testowego konsumenta dla `raw_transactions` -->

## Should-have

- [ ] Dodać pamięć lokalną dla częstych lokalizacji w detektorze, zgodnie z wymaganiami projektu.
<!-- TODO: Dodać pamięć lokalną dla częstych lokalizacji w detektorze -->
- [ ] Podłączyć MongoDB do realnego zapisu alarmów albo statystyk, bo kontener istnieje w docker-compose.yaml, ale kod go nie używa.
<!-- TODO: Podłączyć MongoDB do realnego zapisu alarmów albo statystyk -->
- [ ] Dodać obsługę co najmniej jeszcze jednej anomalii poza skokiem kwoty, np. gwałtowna zmiana lokalizacji i zbyt wysoka częstotliwość transakcji.
<!-- TODO: Dodać obsługę co najmniej jeszcze jednej anomalii poza skokiem kwoty -->

## Nice-to-have

- [ ] Rozbudować app.py o lepszą wizualizację alarmów i historii, zamiast samego podglądu na żywo.
<!-- TODO: Rozbudować aplikację o lepszą wizualizację alarmów i historii -->
- [ ] Dodać automatyczne tworzenie topiców Kafki przy starcie albo skrypt inicjalizacyjny.
<!-- TODO: Dodać automatyczne tworzenie topiców Kafki przy starcie albo skrypt inicjalizacyjny -->
- [ ] Uporządkować konfigurację i dokumentację uruchomienia lokalnego, żeby było jasno: host vs kontenery, jaki port do czego, co startuje w Dockerze, a co lokalnie.
<!-- TODO: Uporządkować konfigurację i dokumentację uruchomienia lokalnego.
Host vs kontenery, jaki port do czego, co startuje w Dockerze, a co lokalnie -->
