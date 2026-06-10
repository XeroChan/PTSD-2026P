from pyflink.datastream import StreamExecutionEnvironment

from src.flink_pipeline.core.pipeline_builder import FraudDetectionPipeline


def run_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    FraudDetectionPipeline(env).build()
    env.execute("Fraud_Detection_Job")


if __name__ == '__main__':
    run_job()
