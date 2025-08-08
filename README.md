# System Architecture

![System Architecture](/Docs/full_arch.png)
# 🐳 Dockerized Data Engineering Stack

A fully integrated Docker-based environment for building modern data engineering pipelines with:

- **Apache Airflow**
- **Apache Kafka + Zookeeper**
- **Apache Hadoop (HDFS + YARN)**
- **PostgreSQL (Airflow & Application)**
- **Jupyter Notebook with PySpark**



## 🔧 Services Summary
#### 🌐 Internal Network Configuration
All containers are connected to the custom Docker bridge network `sparknet` with a static IP setup in the `172.30.0.0/16` subnet

| Service                  | Description                                         | Container Name        | Internal IP     | Host IP     | Port Mapping                   | Username     | Password     |
|--------------------------|-----------------------------------------------------|------------------------|------------------|-------------|-------------------------------|--------------|--------------|
| **Airflow Webserver**    | Airflow UI and REST API                             | `airflow-webserver`    | `172.30.1.15`    | `localhost` | `18080:8080`                   | `airflow`    | `airflow`    |
| **Airflow Scheduler**    | DAG execution engine                                | `airflow-scheduler`    | `172.30.1.16`    | Internal    | -                             | `airflow`    | `airflow`    |
| **Airflow Triggerer**    | Async task handler                                  | `airflow-triggerer`    | `172.30.1.17`    | Internal    | -                             | `airflow`    | `airflow`    |
| **Airflow CLI**          | Airflow terminal commands                           | `airflow-cli`          | `172.30.1.20`    | Internal    | -                             | `airflow`    | `airflow`    |
| **Airflow Init**         | DB initialization + user bootstrap                  | `airflow-init`         | `172.30.1.18`    | Internal    | -                             | `airflow`    | `airflow`    |
| **PostgreSQL (Airflow)** | Metadata DB for Airflow                             | `postgres_airflow`     | `172.30.1.14`    | Internal    | -                             | `airflow`    | `airflow`    |
| **PostgreSQL (App)**     | General-purpose app DB                              | `postgres_v2`          | `172.30.1.12`    | `localhost` | `5433:5432`                   | `spark`      | `spark`      |
| **Zookeeper**            | Kafka coordination                                  | `zookeeper_v2`         | `172.30.1.10`    | `localhost` | `2181:2181`                   | N/A          | N/A          |
| **Kafka**                | Kafka broker for stream ingestion                   | `kafka_v2`             | `172.30.1.11`    | `localhost` | `9092`, `19092:19092`         | N/A          | N/A          |
| **Jupyter Notebook**     | PySpark-enabled notebook environment                | `spark-jupyter`        | `172.30.1.13`    | `localhost` | `8899:8888`, `4040:4040`      | N/A          | N/A          |
| **Hadoop NameNode**      | Master node for HDFS                                | `hadoop-namenode`      | `172.30.1.21`    | `localhost` | `9870:9870`, `9000:9000`      | N/A          | N/A          |
| **Hadoop DataNode 1**    | HDFS data storage node                              | `hadoop-datanode1`     | `172.30.1.22`    | `localhost` | `9864:9864`, `9866`, `9867`   | N/A          | N/A          |
| **Hadoop DataNode 2**    | HDFS data storage node                              | `hadoop-datanode2`     | `172.30.1.23`    | Internal    | `9865`, `9868`, `9869`        | N/A          | N/A          |
| **ResourceManager**      | YARN job scheduling                                 | `hadoop-resourcemanager`| `172.30.1.24`    | `localhost` | `8088:8088`                   | N/A          | N/A          |
| **NodeManager**          | YARN container execution                            | `hadoop-nodemanager`   | `172.30.1.25`    | Internal    | -                             | N/A          | N/A          |



## 🔗 Web Interfaces

| Component                 | URL                                   | Notes                                |
|---------------------------|----------------------------------------|--------------------------------------|
| **Airflow UI**            | [http://localhost:18080](http://localhost:18080) | Use `airflow/airflow` to log in     |
| **Jupyter Notebook (Lab)**| [http://localhost:8899](http://localhost:8899)   | No token required                    |
| **Spark UI**              | [http://localhost:4040](http://localhost:4040)   | Visible during active Spark jobs    |
| **HDFS NameNode UI**      | [http://localhost:9870](http://localhost:9870)   | File system browser                  |
| **YARN ResourceManager**  | [http://localhost:8088](http://localhost:8088)   | Job monitoring & container status   |






## 📦 Docker Volumes

These ensure **persistent storage**:

| Volume Name         | Used By           | Container Path                | Purpose                       |
|---------------------|-------------------|--------------------------------|--------------------------------|
| `postgres-db-volume`| `postgres_airflow`| `/var/lib/postgresql/data`    | Airflow metadata DB            |
| `pgdata`            | `postgres`        | `/var/lib/postgresql/data`    | Application PostgreSQL DB      |
| `hadoop-namenode`   | `hadoop-namenode` | `/hadoop/dfs/name`            | HDFS NameNode metadata         |
| `hadoop-datanode1`  | `hadoop-datanode1`| `/hadoop/dfs/data`            | HDFS DataNode 1 data           |
| `hadoop-datanode2`  | `hadoop-datanode2`| `/hadoop/dfs/data`            | HDFS DataNode 2 data           |
| `grafana-storage`   | `grafana`         | `/var/lib/grafana`            | Grafana dashboards & configs   |






## 🔗 Bind Mounts

These map **host directories** to container paths for code, configs, and data sharing:

| Host Path                           | Used By         | Container Path         | Purpose                           |
|-------------------------------------|-----------------|------------------------|------------------------------------|
| `${AIRFLOW_PROJ_DIR:-.}/dags`       | All Airflow svc | `/opt/airflow/dags`    | Airflow DAG scripts               |
| `${AIRFLOW_PROJ_DIR:-.}/logs`       | All Airflow svc | `/opt/airflow/logs`    | Airflow task logs                  |
| `${AIRFLOW_PROJ_DIR:-.}/config`     | All Airflow svc | `/opt/airflow/config`  | Airflow configuration files        |
| `${AIRFLOW_PROJ_DIR:-.}/plugins`    | All Airflow svc | `/opt/airflow/plugins` | Custom Airflow plugins              |
| `${AIRFLOW_PROJ_DIR:-.}/data`       | All Airflow svc | `/opt/airflow/data`    | Shared data between services        |
| `${AIRFLOW_PROJ_DIR:-.}`            | `airflow-init`  | `/sources`             | Initialization (DAGs, logs, plugins)|
| `./notebooks`                       | `jupyter`       | `/home/jovyan/work`    | Jupyter notebooks workspace         |
| `./jars`                            | `jupyter`       | `/opt/spark/jars`      | Spark extra JAR dependencies        |
| `./data`                            | `jupyter`       | `/opt/airflow/data`    | Shared data between Jupyter & Airflow|





## 📁 Project Folder Structure

```text
.
├── dags/             ← Airflow DAGs
├── logs/             ← Airflow logs
├── config/           ← Optional Airflow configs
├── plugins/          ← Custom Airflow plugins
├── data/             ← Input/output data folder
├── notebooks/        ← Jupyter notebooks
├── jars/             ← Spark JARs (for Kafka, hadoop, etc.)
├── docker-compose.yml
└── file.md           ← This documentation

```
## Connect with me

- [🔗 LinkedIn Account  →    WWW.linkedin.com/mohamed-eldeeb](https://www.linkedin.com/in/mohamed-eldeeb-9706261b6/)





