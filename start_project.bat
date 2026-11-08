@echo off
echo  1. Allumage de l'usine (Docker)...
docker-compose up -d

echo  2. Attente de l'initialisation de Cassandra ...
:WAIT_CASSANDRA
docker exec cassandra cqlsh -e "SHOW VERSION" >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    timeout /t 5 /nobreak > NUL
    echo ... Cassandra demarre toujours ...
    goto WAIT_CASSANDRA
)

echo  3. Cassandra est PRET ! Creation de la base de donnees...
docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS netshield WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec cassandra cqlsh -e "DROP TABLE IF EXISTS netshield.alertes;"
docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS netshield.alertes ( label text, destination_port text, count bigint, PRIMARY KEY (label, destination_port) );"

echo  4. Demarrage de Hadoop sur le Master...
docker exec -d hadoop-master ./start-hadoop.sh

echo  Attente que Hadoop soit pret (15 secondes)...
timeout /t 15 /nobreak > NUL

echo  5. Lancement du job Spark Streaming en arriere-plan...
docker exec -d hadoop-master bash -c "export PYSPARK_PYTHON=python3 && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.12 /src/spark_job.py"

echo  6. Lancement du simulateur de trafic reseau (en arriere-plan)...
start /B python src\simulateur_reseau.py

echo  7. Lancement du Dashboard (Interface Graphique)...
python -m streamlit run src\app.py