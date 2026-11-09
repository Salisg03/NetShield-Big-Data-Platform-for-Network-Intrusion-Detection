@echo off
echo  1. Allumage de l'usine (Docker)...
docker-compose up -d

echo  2. Attente de Cassandra...
:WAIT_CASSANDRA
docker exec cassandra cqlsh -e "SHOW VERSION" >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    timeout /t 5 /nobreak > NUL
    echo ... Cassandra demarre toujours ...
    goto WAIT_CASSANDRA
)

echo  3. Creation des DEUX tables (Temps Reel et Historique)...
docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS netshield WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS netshield.alertes ( label text, destination_port text, count bigint, PRIMARY KEY (label, destination_port) );"
docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS netshield.historique ( label text, destination_port text, count bigint, PRIMARY KEY (label, destination_port) );"

echo  4. Demarrage de Hadoop sur le Master...
docker exec -d hadoop-master ./start-hadoop.sh
timeout /t 10 /nobreak > NUL

echo  5. Chargement de l'echantillon dans HDFS (Le Data Lake)...
docker exec hadoop-master hdfs dfs -mkdir -p /data/cyber
docker exec hadoop-master bash -c "hdfs dfs -put -f /dataset/echantillon_historique.csv /data/cyber/"

echo  6. Lancement du Job BATCH (Historique complet)...
start /B docker exec hadoop-master bash -c "export PYSPARK_PYTHON=python3 && spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 /src/job_historique.py"

echo  Attente de 15 secondes pour laisser le temps au Job Batch de demarrer...
timeout /t 15 /nobreak > NUL

echo  7. Lancement du Job STREAMING (Temps Reel)...
start /B docker exec hadoop-master bash -c "export PYSPARK_PYTHON=python3 && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 /src/job_real_time.py"

echo  Attente de 15 secondes pour l'initialisation de Spark Streaming...
timeout /t 15 /nobreak > NUL

echo  8. Lancement du simulateur de trafic reseau...
start /B python src\simulateur_reseau.py

echo  9. Lancement du Dashboard (Interface Graphique)...
python -m streamlit run src\app.py