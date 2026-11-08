from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import time

# 1. INITIALISATION DU CERVEAU SPARK
spark = SparkSession.builder \
    .appName("NetShield-RealTime") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Demarrage du moteur Spark Streaming (Version 2.2.0)...")

# 2. SCHEMA PARFAITEMENT PROPRE (Sans espaces !)
schema_trafic = StructType([
    StructField("Destination Port", StringType(), True),
    StructField("Label", StringType(), True)
])

# 3. LECTURE KAFKA
flux_brut = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "trafic-reseau") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. TRANSFORMATION (Binaire -> Texte -> DataFrame)
flux_json = flux_brut.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_trafic).alias("data")) \
    .select("data.*")

# 5. AGREGATION (On groupe par Label et par Port)
alertes_agregees = flux_json \
    .groupBy(
        col("Label").alias("label"),
        col("Destination Port").alias("destination_port")
    ) \
    .count()

# 6. ECRITURE EN MEMOIRE RAM (Sans checkpoint pour repartir à zero à chaque fois)
requete = alertes_agregees.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("table_alertes_memoire") \
    .start()

# 7. BOUCLE D'ENVOI VERS CASSANDRA
print("Ecoute en cours... Transfert vers Cassandra actif.")

while requete.isActive:
    time.sleep(3)
    
    # On lit le resultat en RAM
    df_resultat = spark.sql("SELECT * FROM table_alertes_memoire")
    
    # ON AFFICHE LE RESULTAT DANS LE TERMINAL POUR VERIFIER !
    df_resultat.show()
    
    # On ecrit dans Cassandra
    df_resultat.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="alertes", keyspace="netshield") \
        .save()