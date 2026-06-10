import os
import json
import pathlib

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer
)

from src.config.settings import KAFKA_BROKER, TOPIC_RAW, TOPIC_ALARMS, FLINK_GROUP_ID
from src.flink_pipeline.detectors.limit_detector import LimitAnomalyDetector
from src.flink_pipeline.detectors.location_detector import LocationAnomalyDetector
from src.flink_pipeline.detectors.amount_stats_detector import AmountStatsAnomalyDetector

def run_job():
    env = StreamExecutionEnvironment.get_execution_environment()

    jars = [
        "/app/jars/flink-connector-kafka-1.17.0.jar",
        "/app/jars/kafka-clients-3.2.3.jar"
    ]
    
    for jar_path in jars:
        if os.path.exists(jar_path):
            jar_uri = pathlib.Path(jar_path).resolve().as_uri()
            env.add_jars(jar_uri)
            print(f"[OK] Loaded: {jar_path}")
        else:
            print(f"[ERROR] Missing JAR: {jar_path}")

    env.add_python_file("/app/src")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(TOPIC_RAW) \
        .set_group_id(FLINK_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    raw_stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.for_monotonous_timestamps(), 
        "Kafka Raw Transactions"
    )

    limit_alarms = raw_stream \
        .process(LimitAnomalyDetector(), output_type=Types.STRING()) \
        .name("Limit Anomaly Detector")

    location_alarms = raw_stream \
        .key_by(lambda x: json.loads(x)['card_id']) \
        .process(LocationAnomalyDetector(), output_type=Types.STRING()) \
        .name("Location Anomaly Detector")

    amount_stats_alarms = raw_stream \
        .key_by(lambda x: json.loads(x)['card_id']) \
        .process(AmountStatsAnomalyDetector(), output_type=Types.STRING()) \
        .name("Amount Z-Score Detector")

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_ALARMS)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    limit_alarms.sink_to(kafka_sink).name("Kafka Sink - Limit Alarms")
    location_alarms.sink_to(kafka_sink).name("Kafka Sink - Location Alarms")
    amount_stats_alarms.sink_to(kafka_sink).name("Kafka Sink - Amount Stats Alarms")

    env.execute("Fraud_Detection_Job")

if __name__ == '__main__':
    run_job()