# Priorytety

## Must-have
   - Domknąć generator transakcji w producer.py tak, żeby spełniał wymagania danych: 10 000 kart, wielu kart na jednego użytkownika, JSON, kilka typów anomalii.
   - Przerobić flink-job.py na realny detektor, a nie prototyp z `speed = 1500`; trzeba liczyć metryki i wykrywać anomalie statystycznie.
   - Ujednolicić konfigurację Kafki dla hosta i kontenerów, najlepiej przez zmienne środowiskowe, bo teraz producer.py i app.py łączą się z `localhost:9092`, a flink-job.py z `kafka:29092`.
   - Dodać osobnego testowego konsumenta dla `raw_transactions`, bo app.py dziś pokazuje tylko `alarms`.

## Should-have
   - Dodać pamięć lokalną dla częstych lokalizacji w detektorze, zgodnie z wymaganiami projektu.
   - Podłączyć MongoDB do realnego zapisu alarmów albo statystyk, bo kontener istnieje w docker-compose.yaml, ale kod go nie używa.
   - Dodać obsługę co najmniej jeszcze jednej anomalii poza skokiem kwoty, np. gwałtowna zmiana lokalizacji i zbyt wysoka częstotliwość transakcji.

## Nice-to-have
   - Rozbudować app.py o lepszą wizualizację alarmów i historii, zamiast samego podglądu na żywo.
   - Dodać automatyczne tworzenie topiców Kafki przy starcie albo skrypt inicjalizacyjny.
   - Uporządkować konfigurację i dokumentację uruchomienia lokalnego, żeby było jasno: host vs kontenery, jaki port do czego, co startuje w Dockerze, a co lokalnie.