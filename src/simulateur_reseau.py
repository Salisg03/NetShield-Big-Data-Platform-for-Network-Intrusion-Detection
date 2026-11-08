import csv
import time
import json
import glob
from kafka import KafkaProducer

print("Connexion à Kafka en cours...")
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'trafic-reseau'
fichiers_csv = glob.glob('dataset/*.csv')
print(f" {len(fichiers_csv)} fichiers CSV trouvés. Début de la simulation...")

compteur = 0

for fichier in fichiers_csv:
    print(f"\n --- Ouverture du fichier : {fichier} ---")
    
    with open(fichier, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for ligne in reader:
            # NETTOYAGE DES CLÉS 
            # On enlève les espaces au début/fin et les caractères invisibles
            ligne_propre = {}
            for cle, valeur in ligne.items():
                if cle is not None:
                    cle_propre = str(cle).strip().replace('\ufeff', '')
                    # On nettoie aussi la valeur !
                    valeur_propre = str(valeur).strip() if valeur else ""
                    ligne_propre[cle_propre] = valeur_propre
            
            producer.send(TOPIC_NAME, ligne_propre)
            compteur += 1
            
            if compteur % 500 == 0:
                print(f" {compteur} paquets envoyés à Kafka...")
            
            time.sleep(0.001)

print("\n Tous les fichiers ont été lus et envoyés !")
producer.close()