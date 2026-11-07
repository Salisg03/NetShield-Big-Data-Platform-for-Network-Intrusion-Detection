from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import time

spark = SparkSession.builder \
    .appName("NetShield-RealTime") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Demarrage du moteur Spark Streaming (Version 2.2.0)...")

schema_trafic = StructType([
    StructField(" Source IP", StringType(), True),
    StructField(" Destination Port", StringType(), True),
    StructField(" Label", StringType(), True)
])

# 1. LECTURE KAFKA
flux_brut = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "trafic-reseau") \
    .option("startingOffsets", "earliest") \
    .load()

flux_json = flux_brut.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_trafic).alias("data")) \
    .select("data.*")

# 2. AGRÉGATION
alertes_agregees = flux_json \
    .groupBy(col(" Label").alias("Label")) \
    .count()

# 3. L'ASTUCE SPARK 2.2 : On ecrit le resultat en memoire RAM (table virtuelle)
requete = alertes_agregees.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("table_alertes_memoire") \
    .start()

# 4. BOUCLE MANUELLE : Toutes les 3 secondes, on lit la RAM et on envoie a Cassandra !
print("Ecoute en cours... Transfert vers Cassandra active.")

while requete.isActive:
    time.sleep(3)
    # On recupere les donnees calculees par le Stream
    df_resultat = spark.sql("SELECT * FROM table_alertes_memoire")
    
    # On affiche dans le terminal
    df_resultat.show()
    
    # On sauvegarde dans Cassandra (Le mode append fera un "Upsert" grace a la Primary Key)
    df_resultat.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="alertes", keyspace="netshield") \
        .save()