x-airflow-common: &airflow-common
  image: apache/airflow:2.7.1-python3.9
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__WEBSERVER__SECRET_KEY: "my_secret_key"
    AIRFLOW__WEBSERVER__WORKERS: "1"
    AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT: "300"
    SPARK_MASTER_URL: spark://spark-master:7077
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-}
    AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-us-east-1}
    _PIP_ADDITIONAL_REQUIREMENTS: "pyspark apache-airflow-providers-apache-spark boto3"
  volumes:
    - ../batch/airflow/dags:/opt/airflow/dags
    - ../batch/airflow/logs:/opt/airflow/logs
    - ../batch/airflow/plugins:/opt/airflow/plugins
    - ../batch/spark_jobs:/opt/airflow/batch/spark_jobs
    - ../deployment/scripts:/scripts
    - /var/run/docker.sock:/var/run/docker.sock
  user: "${AIRFLOW_UID:-50000}:0"
  networks:
    - glowpipe_network
  depends_on:
    postgres:
      condition: service_healthy
    redis:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    container_name: airflow_postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - glowpipe_network

  redis:
    image: redis:latest
    container_name: airflow_redis
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - glowpipe_network

  spark-master:
    image: apache/spark-py:latest
    container_name: spark_master
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
      --host spark-master
      --port 7077
      --webui-port 8080
    ports:
      - "7077:7077"
      - "8083:8080"
    networks:
      - glowpipe_network

  spark-worker:
    image: apache/spark-py:latest
    container_name: spark_worker
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker
        spark://spark-master:7077
        --cores 1
        --memory 1G
        --webui-port 8081
    depends_on:
      - spark-master
    ports:
      - "8081:8081"
    networks:
      - glowpipe_network

  airflow-init:
    <<: *airflow-common
    container_name: airflow_init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init
        airflow users create \
          --username airflow \
          --password airflow \
          --firstname Airflow \
          --lastname Admin \
          --role Admin \
          --email airflow@example.com || true
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow_webserver
    command: airflow webserver
    ports:
      - "8082:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully
      spark-master:
        condition: service_started

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow_scheduler
    command: airflow scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully
      spark-master:
        condition: service_started

  airflow-worker:
    <<: *airflow-common
    container_name: airflow_worker
    command: airflow celery worker
    depends_on:
      airflow-init:
        condition: service_completed_successfully
      spark-master:
        condition: service_started

volumes:
  postgres_data:

networks:
  glowpipe_network:
    driver: bridge