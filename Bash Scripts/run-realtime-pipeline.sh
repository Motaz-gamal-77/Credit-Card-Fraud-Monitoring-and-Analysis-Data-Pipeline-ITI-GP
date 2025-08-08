#!/bin/bash
pip install papermill


pip install ipykernel

PID_FILE="pipeline.pids"
> "$PID_FILE"  # Clear previous PIDs

papermill /home/jovyan/work/kafka-connections/Topic.ipynb /home/jovyan/work/kafka-connections/Topic.ipynb -k python3 --log-output


# Run Kafka producer

papermill /home/jovyan/work/kafka-connections/producer.ipynb /home/jovyan/work/kafka-connections/producer.ipynb -k python3 --log-output &
echo $! > "$PID_FILE"     # Save PID of producer

# Run Spark consumer
papermill /home/jovyan/work/Spark-Streaming/Cleaning_Treansformations.ipynb /home/jovyan/work/Spark-Streaming/Cleaning_Treansformations.ipynb -k python3 --log-output &
echo $! >> "$PID_FILE"    # Save PID of consumer


# Run Spark_hdfs consumer
papermill /home/jovyan/work/Spark-Streaming/readKafkaWriteHadoop.ipynb /home/jovyan/work/Spark-Streaming/readKafkaWriteHadoop.ipynb -k python3 --log-output &
echo $! >> "$PID_FILE"    # Save PID of consumer

echo "Producer and Consumer started. PIDs saved in $PID_FILE"


