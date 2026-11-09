import os
import glob
import csv
import time

fichiers_csv = glob.glob('dataset/*.csv')
fichiers_csv = [f for f in fichiers_csv if "echantillon_historique" not in f]

fichier_echantillon = 'dataset/echantillon_historique.csv'

print("Création de l'échantillon ultra-rapide (300 Attaques, 700 Sain)...")

CIBLE_SAIN = 700
CIBLE_ATTAQUES = 300

compte_sain = 0
compte_attaques = 0

with open(fichier_echantillon, 'w', encoding='utf-8') as f_out:
    header_written = False
    
    for fichier in fichiers_csv:
        if compte_sain >= CIBLE_SAIN and compte_attaques >= CIBLE_ATTAQUES:
            break 
            
        with open(fichier, 'r', encoding='utf-8') as f_in:
            reader = csv.reader(f_in)
            entete = next(reader)
            
            if not header_written:
                f_out.write(','.join(entete) + '\n')
                header_written = True
                
            for ligne in reader:
                if len(ligne) < 79: continue 
                
                label = ligne[-1].strip()
                
                if label == "BENIGN" and compte_sain < CIBLE_SAIN:
                    f_out.write(','.join(ligne) + '\n')
                    compte_sain += 1
                elif label != "BENIGN" and compte_attaques < CIBLE_ATTAQUES:
                    f_out.write(','.join(ligne) + '\n')
                    compte_attaques += 1
                    
                if compte_sain >= CIBLE_SAIN and compte_attaques >= CIBLE_ATTAQUES:
                    break

print(f" Échantillon créé avec succès : {compte_sain} sains, {compte_attaques} malveillants !")

# On laisse 1 petite seconde à Windows pour bien écrire le fichier sur le disque
time.sleep(1)