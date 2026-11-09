from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("NetShield-Batch-Historique") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# CORRECTION : PLUS AUCUN ACCENT !
print("Demarrage du Job Batch (Analyse Historique HDFS)...")

df_brut = spark.read \
    .option("header", "true") \
    .csv("hdfs://hadoop-master:9000/data/cyber/*.csv")

df_historique = df_brut.groupBy(
    col(" Label").alias("label"),
    col(" Destination Port").alias("destination_port")
).count()

df_historique.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="historique", keyspace="netshield") \
    .save()

# CORRECTION : PLUS AUCUN ACCENT !
print("Job Batch termine ! L'historique est dans Cassandra.")
spark.stop()