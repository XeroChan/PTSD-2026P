import os

# Adres brokera Kafki. Domyślnie 'kafka:29092' (komunikacja wewnątrz sieci Dockera)
# Uruchamiając z hosta, ustaw zmienną środowiskową, np. KAFKA_BROKER=localhost:9092
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")

TOPIC_RAW = os.environ.get("TOPIC_RAW", "raw_transactions")
TOPIC_ALARMS = os.environ.get("TOPIC_ALARMS", "alarms")
FLINK_GROUP_ID = os.environ.get("FLINK_GROUP_ID", "flink-anomaly-group")

# MongoDB. Domyślnie 'mongo:27017' (usługa w sieci Dockera)
# Z hosta: MONGO_URI=mongodb://localhost:27017
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "fraud_detection")
MONGO_COLLECTION_ALARMS = os.environ.get("MONGO_COLLECTION_ALARMS", "alarms")