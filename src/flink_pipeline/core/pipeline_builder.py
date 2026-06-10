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
    KafkaOffsetsInitializer,
)

from src.config.settings import KAFKA_BROKER, TOPIC_RAW, TOPIC_ALARMS, FLINK_GROUP_ID
from src.flink_pipeline.detectors.limit_detector import LimitAnomalyDetector
from src.flink_pipeline.detectors.location_detector import LocationAnomalyDetector
from src.flink_pipeline.detectors.amount_stats_detector import AmountStatsAnomalyDetector

JARS = [
    "/app/jars/flink-connector-kafka-1.17.0.jar",
    "/app/jars/kafka-clients-3.2.3.jar",
]

# Równoległość detektorów (rozkładają się na sloty TaskManagera).
DETECTOR_PARALLELISM = 4


class FraudDetectionPipeline:
    """
    Buduje topologię Flinka: źródło Kafki -> trzy detektory anomalii ->
    połączony, uporządkowany sink alarmów.

    Oddziela konstrukcję potoku od jego uruchomienia (to robi job.py).
    """

    def __init__(self, env: StreamExecutionEnvironment):
        self.env = env

    def build(self) -> None:
        self._configure_environment()
        raw_stream = self._build_source()
        alarms = self._build_detectors(raw_stream)
        self._build_sink(alarms)

    def _configure_environment(self) -> None:
        for jar_path in JARS:
            if os.path.exists(jar_path):
                self.env.add_jars(pathlib.Path(jar_path).resolve().as_uri())
                print(f"[OK] Loaded: {jar_path}")
            else:
                print(f"[ERROR] Missing JAR: {jar_path}")
        self.env.add_python_file("/app/src")
        self.env.set_parallelism(DETECTOR_PARALLELISM)

    def _build_source(self):
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(KAFKA_BROKER) \
            .set_topics(TOPIC_RAW) \
            .set_group_id(FLINK_GROUP_ID) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        return self.env.from_source(
            kafka_source,
            WatermarkStrategy.for_monotonous_timestamps(),
            "Kafka Raw Transactions",
        )

    def _build_detectors(self, raw_stream):
        keyed_stream = raw_stream.key_by(lambda x: json.loads(x)['card_id'])

        limit_alarms = raw_stream \
            .process(LimitAnomalyDetector(), output_type=Types.STRING()) \
            .name("Limit Anomaly Detector")

        location_alarms = keyed_stream \
            .process(LocationAnomalyDetector(), output_type=Types.STRING()) \
            .name("Location Anomaly Detector")

        amount_stats_alarms = keyed_stream \
            .process(AmountStatsAnomalyDetector(), output_type=Types.STRING()) \
            .name("Amount Z-Score Detector")

        # Wszystkie alarmy w jeden strumień -> jeden uporządkowany sink.
        return limit_alarms.union(location_alarms, amount_stats_alarms)

    def _build_sink(self, alarms) -> None:
        kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers(KAFKA_BROKER) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(TOPIC_ALARMS)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
            ) \
            .build()

        # Równoległość 1 dla uporządkowanego, przeplatanego wyjścia.
        alarms.sink_to(kafka_sink).name("Kafka Sink - Alarms").set_parallelism(1)
