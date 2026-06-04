KAFKA_BROKER = 'kafka:29092'
GROUP_ID = 'flink-group'
TOPIC_RAW = 'raw_transactions'
TOPIC_ALARMS = 'alarms'

# Configuration for anomaly detection
MAX_ALLOWED_SPEED_KMH = 1000  # Max speed in km/h before raising impossible travel alarm
