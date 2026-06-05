# To run

docker-compose build \
docker-compose up -d \
docker-compose exec jobmanager flink run -py /app/src/flink_pipeline/job.py
