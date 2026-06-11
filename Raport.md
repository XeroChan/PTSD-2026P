# Raport z Projektu: PTSD-2026P
## System Wykrywania Oszustw Transakcyjnych w Czasie Rzeczywistym (Real-time Fraud Detection System)

### 1. Wstęp i Cel Projektu
Celem projektu było zaprojektowanie oraz implementacja rozproszonego systemu do przetwarzania strumieniowego, którego zadaniem jest wykrywanie anomalii i potencjalnych oszustw w transakcjach kartowych (Fraud Detection) w czasie rzeczywistym. System weryfikuje strumień zdarzeń pod kątem typowych scenariuszy ataków, takich jak kradzież karty, klonowanie, czy nagłe i nietypowe zmiany w wydatkach.

Projekt został zrealizowany w oparciu o architekturę sterowaną zdarzeniami (Event-Driven Architecture) i wykorzystuje nowoczesny stos technologiczny oparty m.in. na Apache Flink, Apache Kafka oraz systemach noSQL.

### 2. Architektura Systemu
System składa się z luźno powiązanych ze sobą mikroserwisów, skonteneryzowanych przy użyciu platformy Docker, co zapewnia łatwość wdrożenia i odtwarzalność środowiska. Główne komponenty to:

* **Symulator Transakcji (Data Generator)**: Moduł napisany w języku Python, symulujący realistyczny ruch z ponad 10 000 unikalnych kart płatniczych. Generator wstrzykuje do głównego strumienia zarówno transakcje prawidłowe, jak i różne typy anomalii.
* **Apache Kafka & Zookeeper**: System kolejkowy pełniący rolę centralnego brokera wiadomości. Gwarantuje niezawodny transfer danych między generatorem a silnikiem przetwarzającym. Skonfigurowano tu m.in. topiki `raw_transactions` oraz `alarms`.
* **Apache Flink (JobManager & TaskManager)**: Klaster przetwarzania strumieniowego. To tutaj zaimplementowano detektory wykorzystujące stanowe przetwarzanie danych (Stateful Stream Processing). Zmodyfikowano pipeline, aby uwzględniał m.in. przesuwna okna (sliding windows) i zapamiętywanie częstych lokalizacji.
* **Alarm Store & MongoDB**: Niezależny mikroserwis (`alarm_store`) działający jako konsument Kafki, którego jedynym zadaniem jest utrwalanie wygenerowanych przez Flinka alarmów w bazie dokumentowej MongoDB.
* **Streamlit Dashboard**: Aplikacja analityczna dla użytkownika końcowego. Wizualizuje w czasie rzeczywistym pojawiające się alarmy, statystyki transakcyjne z wykorzystaniem biblioteki `streamz` (agregacje potokowe) oraz odczytuje historię z MongoDB.

### 3. Przepływ Danych (Data Flow)
Przepływ danych w systemie ma charakter jednokierunkowy i ciągły (Streaming Pipeline):

1. **Ingestia**: Symulator generuje JSON-y reprezentujące transakcje i wysyła je na topik Kafki `raw_transactions`.
2. **Przetwarzanie (Stream Processing)**: Apache Flink na bieżąco konsumuje z topiku `raw_transactions`. Dane są kluczowane po `card_id`, dzięki czemu historia danej karty trafia zawsze do tego samego TaskManagera. Flink przepuszcza każdą transakcję przez zestaw detektorów zaimplementowanych jako `KeyedProcessFunction`.
3. **Generowanie alertów**: W przypadku wykrycia anomalii, Flink generuje obiekt `Alarm` i wysyła go na topik `alarms`.
4. **Utrwalanie (Persistence)**: Usługa `alarm_store` subskrybuje topik `alarms` i zapisuje każdy nowy wpis asynchronicznie w kolekcji MongoDB.
5. **Prezentacja (Visualization)**: Dashboard w Streamlit pełni rolę podwójnego konsumenta:
   * Nasłuchuje Kafka (`alarms` i `raw_transactions`), by odświeżać statystyki na żywo (metody `source.emit(alarm)`).
   * Okresowo odpytuje MongoDB, aby zaprezentować pełną historię naruszeń.

### 4. Wykorzystane Algorytmy i Metody Detekcji
Aby system był skuteczny, zastosowano kilka niezależnych logik biznesowych analizujących ruch pod kątem oszustw:

* **Analiza Statystyczna (Z-Score) - `AMOUNT_ZSCORE_ANOMALY`**: Detektor wykorzystuje stan Flinka do utrzymania przesuwnego okna 30 ostatnich transakcji dla każdej karty. Jeśli nowa kwota transakcji odchyla się od średniej o więcej niż ustalony próg (np. $Z > 4.0$), generowany jest alarm. Dzięki temu chronimy użytkownika przed nietypowymi wydatkami rzędu 90% limitu, które nie zostałyby wychwycone przez zwykłe ograniczenia.
* **Niemożliwa Podróż (Impossible Travel)**: Detektor oblicza odległość (Haversine) oraz czas między bieżącą a ostatnią transakcją, ustalając prędkość przemieszczania się karty. Jeśli przekracza ona np. 1000 km/h, system flaguje to jako sklonowanie karty na innym kontynencie.
* **Nietypowa Lokalizacja (Unusual Location)**: Algorytm uczy się wzorców i zapamiętuje "częste lokalizacje" (obszary, z których wykonano przynajmniej 3 transakcje). Zakupy poza zaufanym promieniem 50 km podnoszą alert ostrzegawczy.
* **Przekroczenie Limitu / Anomalie Częstotliwości**: Proste detektory weryfikujące twarde limity karty (Amount > Limit) oraz zbyt wysoką liczbę transakcji w wąskim oknie czasowym (High Frequency).

### 5. Workflow i Zarządzanie Środowiskiem
Projekt został zautomatyzowany pod kątem wdrożenia na potrzeby środowisk typu POC (Proof of Concept) i deweloperskiego:

* **Docker Compose**: Umożliwia zdefiniowanie całego klastra (baza, broker, processing engine, aplikacja frontendowa, symulator) za pomocą poleceń `docker-compose build` i `docker-compose up -d`.
* **Flink Job Submitter**: Opracowano skrypt narzędziowy, który odczekuje na start klastra Flink, a następnie za pośrednictwem API REST automatycznie zgłasza joba napisanego w PyFlink do klastra, bez konieczności manualnej interwencji.
* **Infrastruktura as Code**: Ujednolicono zarządzanie zmiennymi (np. `KAFKA_BROKER`, `MONGO_URI`), co pozwala aplikacjom na działanie identycznie wewnątrz kontenerów, jak i jako aplikacje wolnostojące w celach debugowania.

### 6. Podsumowanie
Rozwiązanie z powodzeniem demonstruje użycie Apache Flink jako wydajnego silnika przetwarzania zdarzeń wspieranego modelem stanowym. Połączenie rozproszonej kolejki (Kafka) oraz nierelacyjnej bazy danych do szybkiego zapisu logów i logiki (Mongo) pozwoliło stworzyć skalowalny fundament, który może zostać w łatwy sposób wdrożony do infrastruktury chmurowej w środowisku produkcyjnym (np. AWS Kinesis + EMR, lub Confluent Cloud). Modułowa architektura pozwala także na bezproblemowe dopisywanie kolejnych modeli detekcyjnych (w tym detektorów opartych na uczeniu maszynowym) bez konieczności przebudowy istniejących pipeline'ów.
