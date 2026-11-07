from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. INITIALISATION DE SPARK (Le SparkContext)
# C'est l'équivalent du "Driver Program" dans le schéma de l'architecture Spark de ton cours
spark = SparkSession.builder \
    .appName("NetShield-RealTime-IDS") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# On empêche Spark d'afficher trop de logs inutiles dans le terminal
spark.sparkContext.setLogLevel("WARN")

print("Démarrage du moteur d'analyse Spark Streaming...")

# 2. DÉFINITION DU SCHÉMA DES DONNÉES
# Comme on a vu dans le cours (DataFrame/Dataset), définir un schéma offre une sécurité élevée.
# On ne prend que les colonnes qui nous intéressent dans le gros CSV !
schema_trafic = StructType([
    StructField(" Source IP", StringType(), True),       # L'adresse IP de l'attaquant (si dispo)
    StructField(" Destination Port", StringType(), True), # Le port attaqué
    StructField(" Label", StringType(), True)            # Le type de trafic (BENIGN, DDoS, PortScan...)
])

# 3. LECTURE DEPUIS KAFKA (Transformation - Évaluation Paresseuse)
# On se connecte à notre salle d'attente (Kafka)
flux_brut = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "trafic-reseau") \
    .load()

# Les données Kafka arrivent sous forme binaire. On les convertit en texte (String) puis en JSON
flux_json = flux_brut.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_trafic).alias("data")) \
    .select("data.*")


# 4. LA LOGIQUE MÉTIER (TES PATRONS DE CONCEPTION MAPREDUCE APPLIQUÉS)

# PATRON DE FILTRAGE (Ton excellente idée !)
# On élimine le trafic normal, on ne garde QUE les attaques
flux_attaques = flux_json.filter(col(" Label") != "BENIGN")

# PATRON DE RÉCAPITULATION NUMÉRIQUE (Le GroupBy / Reducer)
# On regroupe par Type d'attaque et on compte !
# (Note: En streaming, Spark fait ce "ReduceByKey" en continu en mémoire RAM)
alertes_agregees = flux_attaques \
    .groupBy(" Label") \
    .count()

# 5. L'ACTION : ÉCRITURE DANS CASSANDRA (NoSQL)
# Tant qu'on n'appelle pas une action (comme writeStream), Spark ne fait rien (Lazy Evaluation !)

def ecrire_vers_cassandra(batch_df, batch_id):
    # Cette fonction prend le résultat du calcul et l'insère dans Cassandra
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="alertes", keyspace="netshield") \
        .save()

# On lance la machine ! Spark va mettre à jour les résultats toutes les 5 secondes
requete = alertes_agregees.writeStream \
    .outputMode("update") \
    .foreachBatch(ecrire_vers_cassandra) \
    .start()

# On dit à Spark d'attendre indéfiniment que de nouvelles données arrivent
requete.awaitTermination()