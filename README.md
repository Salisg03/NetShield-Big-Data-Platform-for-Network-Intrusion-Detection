# 🛡️ NetShield : Plateforme Big Data de Détection d'Intrusions Réseau

##  1. Contexte et Objectif
Les infrastructures modernes subissent des cyberattaques en continu. Les systèmes SGBD Relationnels classiques sont incapables d'ingérer et d'analyser ces volumes massifs de trafic réseau en temps réel à cause de leurs contraintes transactionnelles (ACID). 

L'objectif de ce projet est de concevoir une **Architecture Lambda** complète capable de :
1. **Speed Layer (Temps Réel) :** Ingérer un flux réseau massif, détecter les attaques à la volée en mémoire RAM, et alerter instantanément.
2. **Batch Layer (Historique) :** Analyser le "Data Lake" (historique complet des pcap/csv) pour en tirer des tendances globales.
3. **Serving Layer :** Stocker les résultats dans une base NoSQL hautement disponible.
4. **Visualisation :** Exposer ces deux vues (Temps Réel & Batch) via un Dashboard interactif.

##  2. Le Jeu de Données
Nous utilisons le dataset **CIC-IDS-2017** (Université du Nouveau-Brunswick). Il comporte 79 caractéristiques réseau (Port, Durée du flux...) et un label cible (`BENIGN` ou type d'attaque : `DDoS`, `PortScan`, etc.).

---

##  3. Architecture et Choix Technologiques

Notre infrastructure est entièrement conteneurisée (Docker) et repose sur :

1. **Ingestion (Buffer) : Apache Kafka (Confluent)**
   * *Rôle :* Encaisse la charge (pics DDoS) générée par notre simulateur Python et protège le moteur de calcul.
2. **Moteur de Calcul : Apache Spark (PySpark)**
   * *Speed Layer :* **Spark Streaming** lit Kafka, filtre et agrège les attaques en mémoire (Micro-batching).
   * *Batch Layer :* **Spark SQL** lit les Go de données froides dans HDFS.
3. **Stockage Froid (Data Lake) : Hadoop HDFS**
   * *Rôle :* Stockage distribué des fichiers bruts originaux. Garantit la tolérance aux pannes via la réplication de blocs sur les DataNodes.
4. **Stockage Chaud (Résultats) : Apache Cassandra**
   * *Rôle :* Stockage des alertes générées par Spark. 
   * *Justification (Théorème CAP) :* Pour encaisser des milliers de mises à jour par seconde (Upserts), nous avons privilégié la **Disponibilité (A)** et la **Tolérance au Partitionnement (P)**. Cassandra (orienté colonnes) est idéal pour ce cas d'usage comparé à une BDR stricte.
5. **Dashboard : Streamlit**
   * *Rôle :* Interface graphique requêtant Cassandra pour afficher les métriques métiers aux experts cybersécurité.

---

## 📁 4. Structure du Projet

```text
📦 Projet_BigData_NetShield
 ┣ 📂 dataset
 ┃ ┗ 📜 *.csv                 # Fichiers bruts CIC-IDS-2017 (Ignorés par Git)
 ┣ 📂 src
 ┃ ┣ 📜 simulateur_reseau.py  # Script Python : Envoie les CSV dans Kafka
 ┃ ┣ 📜 job_real_time.py      # Job Spark Streaming (Speed Layer)
 ┃ ┣ 📜 job_historique.py     # Job Spark Batch (Batch Layer via HDFS)
 ┃ ┗ 📜 app.py                # Dashboard Streamlit à onglets
 ┣ 📜 docker-compose.yml      # Orchestrateur de l'infrastructure (Hadoop, Kafka, Cassandra)
 ┣ 📜 requirements.txt        # Dépendances Python
 ┣ 📜 start_project.bat       # Script d'automatisation Windows "One-Click"
 ┗ 📜 README.md
 ```

---

##  5. Guide de Déploiement

### Prérequis (Python)
Installez les dépendances nécessaires sur votre machine locale :
```bash
pip install -r requirements.txt
```

### Déploiement automatisé "One-Click" (Windows)
Double-cliquez simplement sur le fichier d'orchestration global :
```text
start_project.bat
```
Ce script se charge de :
1. Lancer l'infrastructure Docker.
2. Attendre intelligemment que Cassandra soit prêt.
3. Créer le *Keyspace* et les tables NoSQL.
4. Lancer Hadoop (YARN/HDFS).
5. Charger les données de test dans le Data Lake (HDFS).
6. Soumettre les deux jobs Spark (Streaming et Batch).
7. Lancer le simulateur de trafic réseau.
8. Ouvrir le Dashboard Streamlit dans le navigateur.
```