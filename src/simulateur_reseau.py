import csv
import time
import json
from kafka import KafkaProducer

# 1. Configuration du Producer Kafka
# Il se connecte au port 9092 (celui qu'on a ouvert dans le docker-compose)
print("Connexion à Kafka en cours...")
producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],  # <-- Changement ici !
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Nom du "tuyau" dans Kafka (le Topic)
TOPIC_NAME = 'trafic-reseau'

# Chemin vers ton fichier téléchargé depuis Kaggle
FICHIER_CSV = 'dataset/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv'

print(f"Début de la simulation du trafic avec le fichier : {FICHIER_CSV}")

# 2. Lecture du fichier et envoi ligne par ligne
with open(FICHIER_CSV, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file) # Lit le CSV en associant les colonnes à leurs noms
    
    compteur = 0
    for ligne in reader:
        producer.send(TOPIC_NAME, ligne)
        compteur += 1
        
        # On n'affiche un message que tous les 500 paquets pour ne pas faire buguer l'écran !
        if compteur % 500 == 0:
            print(f" {compteur} paquets envoyés à Kafka...")
        
        time.sleep(0.009)

print("Fin de la simulation.")
producer.close()