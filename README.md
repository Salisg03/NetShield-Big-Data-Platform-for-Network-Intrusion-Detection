# 🛡️ NetShield : Plateforme Big Data de Détection d'Intrusions Réseau (IDS)

## 📖 1. Contexte et Objectif du Projet
Les infrastructures modernes subissent des cyberattaques en continu (DDoS, Port Scans, etc.). Les systèmes SGBD Relationnels classiques sont incapables d'ingérer et d'analyser ces volumes massifs de trafic réseau en temps réel en raison de leurs contraintes transactionnelles (ACID). 

L'objectif de ce projet est de concevoir une architecture Big Data distribuée (inspirée de la *Lambda Architecture*) capable de :
1. **Ingérer** un flux massif de trafic réseau en temps réel.
2. **Analyser et filtrer** les paquets malveillants à la volée grâce au calcul en mémoire.
3. **Stocker** les alertes dans une base de données NoSQL hautement disponible.
4. **Visualiser** les menaces via un Dashboard interactif.

## 📊 2. Le Jeu de Données
Nous utilisons le dataset **CIC-IDS-2017** (Université du Nouveau-Brunswick), spécifiquement la capture contenant des attaques DDoS du vendredi après-midi. Il comporte 79 caractéristiques réseau (Port, Durée du flux, Taille des paquets...) et un label cible (`BENIGN` ou type d'attaque).

---

## 🏗️ 3. Architecture et Justifications Technologiques

L'architecture est entièrement conteneurisée via Docker et repose sur les piliers suivants :

1. **Ingestion Temps Réel : Apache Kafka (Confluent)**
   * *Rôle :* Agit comme un buffer (tampon) haute performance. Il reçoit les logs du générateur Python et absorbe les pics de charge lors d'une attaque DDoS pour protéger le moteur de calcul.
2. **Moteur de Traitement : Apache Spark (PySpark)**
   * *Rôle :* **Spark Streaming** consomme le flux Kafka, applique un patron de **Filtrage** (isoler les attaques) et de **Récapitulation** (compter par IP sur une fenêtre de temps).
   * *Justification :* Contrairement à MapReduce qui lit/écrit sur disque, Spark utilise les DataFrames pour traiter les données en mémoire (RAM), offrant des performances indispensables pour le temps réel.
3. **Stockage Distribué : Hadoop HDFS**
   * *Rôle :* Data Lake stockant les fichiers bruts originaux pour un archivage sécurisé et distribué (Tolérance aux pannes par réplication de blocs).
4. **Base de Données NoSQL : Apache Cassandra**
   * *Rôle :* Stockage des alertes temps réel. 
   * *Justification (Théorème CAP) :* Pour encaisser des milliers d'alertes par seconde, nous avons privilégié la **Disponibilité (A)** et la **Tolérance au Partitionnement (P)**. Cassandra (orienté colonnes) adopte l'approche **BASE** et offre des vitesses d'écriture extrêmes.
5. **Visualisation : Streamlit**
   * *Rôle :* Interface graphique requêtant Cassandra pour afficher les métriques en temps réel aux administrateurs réseau.

---

## 📁 4. Structure du Projet et Rôle des Fichiers

```text
📦 Projet_BigData_NetShield
 ┣ 📂 dataset
 ┃ ┣ 📜 Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv  # Le trafic réseau brut (Source de données)
 ┃ ┗ 📜 init.cql              # Script d'initialisation de la base NoSQL (Création du Keyspace et Table)
 ┣ 📂 src
 ┃ ┣ 📜 simulateur_reseau.py  # Script Python : Lit le CSV et simule l'envoi du flux en temps réel vers Kafka
 ┃ ┣ 📜 spark_job.py          # Le cerveau : Script Spark qui écoute Kafka, agrège les alertes et écrit dans Cassandra
 ┃ ┗ 📜 app.py                # Dashboard Streamlit (Interface Graphique pour visualiser les attaques)
 ┣ 📜 docker-compose.yml      # Orchestrateur Docker : Construit l'usine complète en déployant les 6 conteneurs
 ┣ 📜 start_project.sh        # Script Bash d'automatisation (Déploiement "One-Click")
 ┗ 📜 README.md               # Ce fichier de documentation
```

---


## 🚀 5. Guide de Démarrage (Comment lancer le projet)

### Méthode 1 : Déploiement automatisé "One-Click" (Recommandé)
Sous Windows, double-cliquez simplement sur le fichier d'orchestration global :
```text
start_project.bat

### Méthode 2 : Déploiement manuel pas-à-pas
Si vous souhaitez lancer les composants un par un :

**1. Démarrer l'infrastructure :**
```bash
docker-compose up -d
```
**2. Initialiser la base NoSQL Cassandra :**
```bash
docker exec -it cassandra cqlsh -f /dataset/init.cql
```
**3. Démarrer Hadoop et le Job Spark Streaming :**
```bash
docker exec -it hadoop-master ./start-hadoop.sh
docker exec -d hadoop-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 /src/spark_job.py
```
**4. Lancer le Dashboard d'Analyses :**
```bash
streamlit run src/app.py
```
**5. Lancer l'attaque réseau (Simulateur) :**
Dans un nouveau terminal, ouvrez les vannes du trafic :
```bash
python src/simulateur_reseau.py
```
*Le tableau de bord web se mettra à jour automatiquement dès la détection de la première attaque !*
```