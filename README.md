# PTSD 2026P - Real-time Fraud Detection System

Projekt implementuje system wykrywania anomalii i oszustw w transakcjach kartowych w czasie rzeczywistym, oparty na architekturze strumieniowej.

## Architektura i Uruchamianie

Wszystkie komponenty systemu zostały spakowane do kontenerów Docker, co ułatwia i ujednolica uruchomienie.

### Co startuje w Dockerze?

Po uruchomieniu `docker-compose up -d`, startują następujące usługi:

- **Zookeeper & Kafka**: Brokery wiadomości (odpowiednio dla koordynacji i strumieniowania). Uruchamiają się z automatycznym skryptem `kafka-setup`, który tworzy niezbędne tematy (np. `raw_transactions`, `alarms`).
- **MongoDB**: Baza danych noSQL służąca do persystencji wykrytych alarmów.
- **Apache Flink (JobManager & TaskManager)**: Silnik przetwarzania strumieniowego.
- **Symulator Transakcji (`simulator`)**: Aplikacja generująca losowy ruch transakcyjny (w tym symulowane anomalie) i wysyłająca go na Kafkę.
- **Streamlit Dashboard (`dashboard`)**: Interaktywny panel wizualizacyjny z bieżącymi i historycznymi alarmami oraz statystykami surowych danych.
- **Alarm Store (`alarm_store`)**: Konsument Kafki, który zapisuje wykryte alarmy na stałe w bazie MongoDB.
- **Flink Job Submitter (`flink-job-submitter`)**: Usługa narzędziowa, która automatycznie zgłasza job przetwarzający Flink po tym, jak klaster Flink zostanie pomyślnie uruchomiony.

### Jak uruchomić system?

```bash
docker compose build
docker compose up -d
```

Job Flinka zostanie zgłoszony automatycznie!

**Ręczne zgłoszenie joba Flink (jeśli potrzebne):**
Jeśli musisz zrestartować job Flinka bez restartowania całego środowiska, wykonaj:

```bash
docker compose exec jobmanager flink run -d -py /app/src/flink_pipeline/job.py
```

### Porty i Dostęp z Hosta (Host vs Kontenery)

Wszystkie usługi komunikują się wewnątrz wirtualnej sieci Dockera (`anomaly_net`), ale wystawiają konkretne porty na hosta, by móc z nich korzystać lokalnie:

- **Streamlit Dashboard**: `http://localhost:8501` - Główny panel analityczny.
- **Apache Flink UI**: `http://localhost:8081` - Podgląd grafu Flink i statusu jobów.
- **Kafka**: `localhost:9092` - Możliwość lokalnego podpięcia się do Kafki z hosta (np. przy użyciu kafkacat lub własnych skryptów Python).
- **MongoDB**: `localhost:27017` - Lokalny dostęp do bazy z hosta.

Dzięki zmiennym środowiskowym (`KAFKA_BROKER`, `MONGO_URI`), aplikacje Python mogą działać zarówno w kontenerze, jak i na hoście, np:

```bash
export KAFKA_BROKER=localhost:9092
export MONGO_URI=mongodb://localhost:27017
```

### Podgląd i Czyszczenie MongoDB

Dane o wykrytych alarmach można podejrzeć włączając się do powłoki MongoDB z kontenera:

```bash
# Wejście do bazy
docker compose exec mongo mongosh

# Komendy wewnątrz mongosh:
use fraud_detection;
db.alarms.find().pretty(); // Podgląd alarmów
db.alarms.countDocuments(); // Zliczenie
db.alarms.drop(); // Czyszczenie bazy!
```

Możesz również podłączyć lokalne GUI (np. MongoDB Compass) na port `localhost:27017` z powyższymi zapytaniami.

### Zatrzymywanie i Czyszczenie Środowiska

Gdy chcesz zakończyć pracę i zatrzymać wszystkie kontenery bez usuwania zachowanych danych (np. historii alarmów w bazie):

```bash
docker compose down
```

**Całkowite usunięcie środowiska (Hard Reset)**
Domyślnie MongoDB zapisuje dane na nazwanym wolumenie Dockera (`mongo_data`), aby przetrwały restarty kontenerów. Jeśli chcesz całkowicie wyczyścić środowisko (usunąć kontenery, sieci oraz wszystkie zapisane dane w bazie, by następnym razem wystartować od zera), dodaj flagę `-v`:

```bash
docker compose down -v
```

> **Uwaga**: Flaga `-v` usunie wolumen `mongo_data` (fizycznie zlokalizowany w katalogach zarządzanych przez silnik Dockera na Twoim komputerze). Po tym poleceniu nie zostanie żaden ślad po działaniu bazy danych ani strumieniach Kafki w systemie.
