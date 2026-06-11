# Raport z Projektu: PSD-2026P

## System Wykrywania Oszustw Transakcyjnych w Czasie Rzeczywistym

### 1. Wstęp i Cel Projektu

Celem projektu była budowa rozproszonego systemu przetwarzania strumieniowego do wykrywania anomalii i oszustw w transakcjach kartowych w czasie rzeczywistym. System sprawdza strumień transakcji pod kątem scenariuszy takich jak kradzież karty, klonowanie i nietypowe zmiany w wydatkach.

System oparto na architekturze sterowanej zdarzeniami i stosie Apache Flink, Apache Kafka oraz MongoDB.

### 2. Architektura Systemu

System składa się z mikroserwisów skonteneryzowanych w Dockerze. Komponenty:

* **Symulator Transakcji**: Moduł w Pythonie generujący ruch z 10 000 kart płatniczych - transakcje prawidłowe oraz kilka typów anomalii - i publikujący go do Kafki.
* **Apache Kafka i Zookeeper**: Broker wiadomości między generatorem a silnikiem przetwarzającym. Topiki: `raw_transactions` i `alarms`.
* **Apache Flink (JobManager i TaskManager)**: Klaster przetwarzania strumieniowego. Detektory korzystają ze stanu Flinka: okna przesuwne i pamięć częstych lokalizacji.
* **Alarm Store i MongoDB**: Mikroserwis `alarm_store` - konsument Kafki, który zapisuje alarmy z Flinka do bazy dokumentowej MongoDB.
* **Streamlit Dashboard**: Aplikacja Streamlit. Wizualizuje alarmy na żywo, agreguje statystyki potokiem `streamz` i odczytuje historię z MongoDB.

### 3. Przepływ Danych

Przepływ danych jest jednokierunkowy i ciągły:

1. **Ingestia**: Symulator generuje JSON-y reprezentujące transakcje i wysyła je na topik Kafki `raw_transactions`.
2. **Przetwarzanie**: Flink konsumuje z topiku `raw_transactions`. Dane są kluczowane po `card_id`, dzięki czemu historia danej karty trafia zawsze do tego samego TaskManagera. Flink przepuszcza każdą transakcję przez zestaw detektorów zaimplementowanych jako `KeyedProcessFunction`.
3. **Generowanie alertów**: Po wykryciu anomalii Flink tworzy obiekt `Alarm` i wysyła go na topik `alarms`.
4. **Utrwalanie**: Usługa `alarm_store` subskrybuje topik `alarms` i zapisuje każdy wpis w kolekcji MongoDB.
5. **Prezentacja**: Dashboard w Streamlit jest podwójnym konsumentem:
   * Nasłuchuje Kafki (`alarms` i `raw_transactions`) i odświeża statystyki na żywo (`source.emit(alarm)`).
   * Okresowo odpytuje MongoDB, by pokazać pełną historię alarmów.

### 4. Wykorzystane Algorytmy i Metody Detekcji

Zastosowano kilka niezależnych detektorów:

* **Analiza statystyczna (z-score) - `AMOUNT_ZSCORE_ANOMALY`**: Stan Flinka przechowuje przesuwne okno 30 ostatnich transakcji karty. Gdy z-score nowej kwoty przekracza próg (Z > 4.0) - alarm. Wychwytuje to nietypowe wydatki (np. 90% limitu), których nie złapie sam próg limitu.
* **Niemożliwa podróż**: Detektor liczy prędkość między ostatnią a bieżącą transakcją (odległość haversine podzielona przez czas). Powyżej 1000 km/h - alarm o możliwym sklonowaniu karty.
* **Nietypowa lokalizacja**: Detektor zapamiętuje częste lokalizacje karty (obszary z co najmniej 3 transakcjami). Transakcja dalej niż 50 km od nich podnosi alarm.
* **Przekroczenie limitu i częstotliwość**: Detektory progowe - kwota większa niż limit oraz zbyt wiele transakcji w krótkim oknie czasowym.

### 5. Uruchamianie i Zarządzanie Środowiskiem

Uruchomienie i konfiguracja są zautomatyzowane:

* **Docker Compose**: Cały system (baza, broker, Flink, symulator, dashboard) startuje poleceniami `docker-compose build` i `docker-compose up -d`.
* **Flink Job Submitter**: Skrypt czeka na start klastra Flink i przez REST API zgłasza job PyFlink - bez ręcznej interwencji.
* **Konfiguracja przez zmienne środowiskowe**: `KAFKA_BROKER`, `MONGO_URI` itd. - ten sam kod działa w kontenerze i jako aplikacja wolnostojąca (debugowanie na hoście).

### 6. Struktura Kodu - Odpowiedzialności Plików i Komponentów

Kod podzielono na warstwy: modele danych (`domain`), logika komponentów (`simulator`, `flink_pipeline`, `persistence`, `ui`), adaptery do systemów zewnętrznych (`infrastructure`) i konfiguracja (`config`).

```powershell
src/
    config/          konfiguracja ze zmiennych środowiskowych
    domain/          modele danych (bez zależności zewnętrznych)
    simulator/       generator transakcji (producent Kafki)
    infrastructure/  adaptery: Kafka, MongoDB
    flink_pipeline/  detektor anomalii (job PyFlink)
        core/        budowa topologii strumienia
        detectors/   4 algorytmy detekcji
    persistence/     utrwalanie alarmów w MongoDB
    ui/              dashboard i agregacje potokowe
    utils/           funkcje pomocnicze (geografia)
```

#### 6.1. Warstwa domenowa - `src/domain/`

| Plik | Odpowiedzialność |
| --- | --- |
| `card.py` | Model karty: `id`, `user_id`, `limit`, `home_location`. Jeden użytkownik może mieć wiele kart (10 000 kart, 5 000 użytkowników). |
| `transaction.py` | Model transakcji: karta, użytkownik, lokalizacja GPS, kwota, dostępny limit, znacznik czasu oraz etykiety wzorcowe (`is_anomaly`, `anomaly_type`) do weryfikacji detekcji. `to_dict()` definiuje kontrakt JSON na topiku Kafki. |
| `location.py` | Współrzędne GPS (`latitude`, `longitude`). |
| `alarm.py` | Model alarmu: typ, karta, czas, słownik `details` ze szczegółami danego detektora. `to_dict()` definiuje kontrakt JSON na topiku `alarms`. |

#### 6.2. Symulator transakcji - `src/simulator/` (komponent b ze specyfikacji)

| Plik | Odpowiedzialność |
| --- | --- |
| `card_generator.py` | Tworzy w pamięci pulę 10 000 kart przypisanych do 5 000 użytkowników (Faker: UUID, współrzędne). Nierównomierny rozkład użycia: 100 "gorących" kart obsługuje 90% ruchu, dzięki czemu detektory stanowe (z-score, lokalizacje) zbierają historię per karta w czasie działania. |
| `transaction_factory.py` | Fabryka transakcji: `create_normal_transaction` (kwota do 20% limitu, lokalizacja blisko domu) oraz trzy anomalie: skok lokalizacji (inny kontynent), przekroczenie limitu (110-200% limitu) i skok kwoty poniżej limitu (50-95% - niewykrywalny progiem, wykrywalny statystycznie). |
| `runner.py` | Pętla producenta: ~10 transakcji na sekundę, losowanie typu (1% lokalizacja, 1% przekroczenie limitu, 1% skok kwoty), a z prawdopodobieństwem 0,5% - seria 5 szybkich transakcji tej samej karty (anomalia częstotliwości). Każda transakcja jest od razu publikowana do Kafki z kluczem `card_id`. |

#### 6.3. Adaptery infrastruktury - `src/infrastructure/`

| Plik | Odpowiedzialność |
| --- | --- |
| `kafka_publisher.py` | Opakowanie producenta `confluent-kafka`: serializacja do JSON, wysyłka z kluczem partycjonującym, callback doręczenia, `flush()` przy zamykaniu. |
| `mongo_repository.py` | Dostęp do MongoDB: `save_alarm()` (zapis), `count()`, `find_recent()` (najnowsze N alarmów, sortowanie po czasie, indeks na `timestamp`), `count_by_type()` (agregacja `$group` po stronie bazy). Używana przez `alarm_store` (zapis) i dashboard (odczyt). |

#### 6.4. Detektor anomalii - `src/flink_pipeline/` (komponent d ze specyfikacji)

| Plik | Odpowiedzialność |
| --- | --- |
| `job.py` | Punkt wejścia joba PyFlink (3 linie logiki): tworzy `StreamExecutionEnvironment`, deleguje budowę topologii do `FraudDetectionPipeline` i uruchamia `env.execute()`. |
| `core/pipeline_builder.py` | Klasa `FraudDetectionPipeline` - budowa topologii: ładowanie JAR-ów konektora Kafki, źródło `KafkaSource` (topik `raw_transactions`, offset `latest` - tylko bieżące zdarzenia), kluczowanie po `card_id`, podpięcie czterech detektorów (równoległość 4 - rozkład na sloty TaskManagera), scalenie ich wyjść operatorem `union` i zapis do jednego `KafkaSink` (topik `alarms`, równoległość 1 dla uporządkowanego wyjścia). |
| `detectors/limit_detector.py` | Bezstanowy `ProcessFunction`: alarm `AMOUNT_LIMIT_EXCEEDED`, gdy kwota większa niż limit. Działa dla każdej karty od pierwszej transakcji. |
| `detectors/location_detector.py` | Stanowy `KeyedProcessFunction` z dwoma sygnałami: pierwszy, `IMPOSSIBLE_TRAVEL` - prędkość między kolejnymi transakcjami karty powyżej 1000 km/h (haversine i różnica czasu), alarm przy każdym takim skoku; drugi, `UNUSUAL_LOCATION` - pamięć częstych lokalizacji (wymóg specyfikacji): stan trzyma do 10 lokalizacji z licznikami wizyt, a transakcja dalej niż 50 km od wszystkich lokalizacji częstych (co najmniej 3 wizyty) podnosi alarm. |
| `detectors/amount_stats_detector.py` | Stanowy detektor statystyczny: stan per karta to przesuwne okno 30 ostatnich kwot. Z okna liczona jest średnia i odchylenie standardowe (wyrażeniem generatorowym), nowa kwota oceniana z-score'em; wartość bezwzględna z powyżej 4 przy min. 12 próbkach daje alarm `AMOUNT_ZSCORE_ANOMALY`. Okno przesuwa się z każdą transakcją, więc detektor zapomina stare dane i nie traci czułości w czasie (w przeciwieństwie do statystyki kumulacyjnej, której odchylenie rośnie monotonicznie). |
| `detectors/frequency_detector.py` | Stanowy detektor częstotliwości: stan trzyma znaczniki czasu transakcji karty z ostatnich 10 sekund; więcej niż 4 transakcje w oknie dają alarm `HIGH_FREQUENCY_TRANSACTIONS`. |

#### 6.5. Utrwalanie alarmów - `src/persistence/` (część komponentu e)

| Plik | Odpowiedzialność |
| --- | --- |
| `alarm_store.py` | Konsument Kafki (grupa `mongo-alarm-store-group`, offset `earliest` - nie gubi alarmów sprzed startu). Każdy alarm zapisuje do MongoDB przez `MongoRepository`. Oddzielony od dashboardu - zapis działa niezależnie od wizualizacji. |

#### 6.6. Wizualizacja - `src/ui/` (komponenty c i e ze specyfikacji)

| Plik | Odpowiedzialność |
| --- | --- |
| `aggregations.py` | Potok strumieniowy `streamz` agregujący alarmy do metryk. Zamiast pętli `for` z `.append()` budowany jest graf `source.map(...).accumulate(...).sink(...)`. Trzy gałęzie: suma kwoty zagrożonej (`accumulate` jako redukcja po strumieniu), licznik alarmów wg typu (`accumulate` z `Counter`), maksymalny z-score (`filter`, `map`, `accumulate`-max). Każdy alarm wpychany jest do potoku przez `source.emit(alarm)`, a agregaty aktualizują się przyrostowo w O(1), bez przeliczania historii. |
| `dashboard.py` | Aplikacja Streamlit z czterema zakładkami o rozdzielonych źródłach danych: Overview (metryki bieżącej sesji z potoku streamz), Alert Details (ostatnie alarmy per typ - bufor po 20 na typ, więc seria jednego typu nie wypycha innych), Raw Transactions (testowy konsument `raw_transactions`, komponent c: liczba przetworzonych transakcji, liczba unikalnych kart, podział normalne i anomalie wg `is_anomaly`, średnia kwota, podgląd surowych JSON-ów), Database History (historia z MongoDB, odświeżana co 5 s). Jeden konsument Kafki subskrybuje oba topiki i w każdej iteracji drenuje wszystkie dostępne wiadomości (pętla `poll` do wyczerpania), więc nie odstaje od tempa produkcji. |

#### 6.7. Konfiguracja i narzędzia

| Plik | Odpowiedzialność |
| --- | --- |
| `config/settings.py` | Konfiguracja: `KAFKA_BROKER`, `MONGO_URI`, nazwy topików i grup - ze zmiennych środowiskowych, z domyślnymi wartościami dla sieci Dockera. Ten sam kod działa w kontenerze (`kafka:29092`) i na hoście (`localhost:9092`). |
| `utils/geo_utils.py` | `haversine_distance()` (odległość po sferze w km) i `calculate_speed_kmh()` (prędkość z dwóch punktów czasoprzestrzennych) - używane przez detektor lokalizacji. |

#### 6.8. Konteneryzacja

| Plik lub usługa | Odpowiedzialność |
| --- | --- |
| `docker-compose.yaml` | Definicja systemu (10 usług) w jednej sieci `anomaly_net`. Kolejność startu wymuszona zależnościami: usługi aplikacyjne czekają, aż `kafka-setup` zakończy się sukcesem (`condition: service_completed_successfully`), co eliminuje wyścig przy tworzeniu topików. Na brokerze wyłączono auto-tworzenie topików (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`), żeby nie powstawały z błędną liczbą partycji. |
| `kafka-setup` (jednorazowy) | Kontener jednorazowy: czeka na gotowość brokera i tworzy topiki - `raw_transactions` z 3 partycjami (równoległy odczyt przez Flinka) i `alarms` z 1 partycją (kolejność alarmów). Kończy z kodem 0. |
| `flink-job-submitter` (jednorazowy) | Kontener jednorazowy: czeka na REST API JobManagera i zgłasza job PyFlink (`flink run -d`). System startuje bez ręcznej interwencji. |
| `Dockerfile.app` | Lekki obraz Pythona dla symulatora, dashboardu i alarm_store - bez zależności PyFlink. Współdzielony przez 3 usługi (`ptsd-2026p-app`). |
| `Dockerfile.flink` | Obraz Flink 1.17 z Pythonem i PyFlink, współdzielony przez JobManager, TaskManager i submitter (`ptsd-2026p-flink`). Pamięć podręczna warstw APT i pip skraca kolejne budowania. |
| `mongo` i wolumen `mongo_data` | Baza dokumentowa z trwałym wolumenem - historia alarmów przeżywa restarty środowiska (czyszczona przez `docker compose down -v`). |

### 7. Strumieniowanie Danych i Operacje Prawie Rzeczywiste

Strumieniowy, a nie wsadowy, charakter przetwarzania obejmuje wszystkie etapy systemu:

**1. Produkcja zdarzeń (`simulator/runner.py`, `infrastructure/kafka_publisher.py`)**
Transakcje są publikowane pojedynczo, od razu po wygenerowaniu (`producer.produce()` i `poll(0)`), z kluczem `card_id`. Klucz gwarantuje, że wszystkie zdarzenia jednej karty trafiają do tej samej partycji - warunek poprawności detekcji stanowej w Flinku.

**2. Transport (Kafka)**
Topik `raw_transactions` ma 3 partycje - strumień jest podzielony, więc Flink czyta równolegle. Topik `alarms` ma 1 partycję: alarmy to uporządkowany w czasie strumień wynikowy.

**3. Silnik detekcji (Flink, `core/pipeline_builder.py` i `detectors/`)**

* **Zdarzenie po zdarzeniu, bez wsadów**: każdy detektor to `process_element()` wywoływany dla pojedynczej transakcji w momencie jej nadejścia - opóźnienie detekcji rzędu milisekund od odczytu z Kafki.
* **Offset `latest`**: job analizuje tylko bieżący strumień (tryb prawie rzeczywisty), nie odtwarza historii topiku przy starcie.
* **Stan kluczowany jako pamięć tymczasowa**: pamięć częstych lokalizacji oraz okna statystyczne są w `ValueState` Flinka - stan żyje przy danych (per `card_id`), nie w zewnętrznej bazie, więc dostęp nie wymaga operacji sieciowych na każde zdarzenie.
* **Okno przesuwne per karta**: detektor z-score liczy statystykę z 30 ostatnich zdarzeń; okno czasowe 10 s w detektorze częstotliwości jest czyszczone przyrostowo przy każdym zdarzeniu.
* **Równoległość**: `env.set_parallelism(4)` i `key_by(card_id)` rozprasza karty na 4 subtaski TaskManagera (4 sloty) - detekcja liczy się współbieżnie. Sink ma `set_parallelism(1)` - kompromis: globalny porządek strumienia alarmów wymaga jednego punktu serializacji.
* **`union` strumieni**: wyjścia czterech detektorów są scalane w jeden potok alarmów zamiast czterech osobnych zapisów.

**4. Utrwalanie (`persistence/alarm_store.py`)**
Konsument działa w pętli (`poll`) i zapisuje każdy alarm w momencie odebrania - opóźnienie od Kafki do MongoDB rzędu milisekund. Offset `earliest` i osobna grupa konsumencka dają komplet historii niezależnie od dashboardu.

**5. Wizualizacja przyrostowa (`ui/aggregations.py`, `ui/dashboard.py`)**

* **Potok `streamz`** to przetwarzanie strumieniowe po stronie konsumenta: metryki (kwota zagrożona, liczniki typów, max z-score) aktualizuje `accumulate` przy każdym `emit` - koszt O(1) na zdarzenie, bez przeliczania historii i bez pętli agregujących.
* **Pętla odbiorcza drenuje topiki do wyczerpania** w każdej iteracji (poll ~0,5 s i `poll(0)` aż do pustki), więc dashboard nadąża za produkcją nawet przy seriach zdarzeń.
* **Rozdzielenie świeżości danych**: zakładki sesyjne (Overview, Alert Details, Raw) aktualizują się w ułamku sekundy od zdarzenia; historia bazodanowa jest odpytywana co 5 s, bo dotyczy danych już utrwalonych.

**Latencja całościowa** (od wygenerowania transakcji do alarmu na dashboardzie) to w praktyce ok. 1 sekundy: symulator publikuje od razu, Flink przetwarza zdarzenie po zdarzeniu, sink emituje alarm zaraz po detekcji, a konsumenci odbierają go w bieżącej iteracji pętli.
