import streamlit as st
import pandas as pd
import time
from cassandra.cluster import Cluster

# 1. Configuration de la page Web
st.set_page_config(page_title="NetShield Dashboard", layout="wide")
st.title(" NetShield - Détection d'Intrusions en Temps Réel")
st.markdown("Ce tableau de bord affiche les attaques détectées par **Apache Spark** et stockées dans **Cassandra**.")

# 2. Connexion à la base de données Cassandra
@st.cache_resource
def get_cassandra_session():
    # Il se connecte au port 9042 qu'on a exposé dans Docker
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect('netshield')
    return session

session = get_cassandra_session()

# 3. Création d'un espace vide qui va se rafraîchir en boucle
placeholder = st.empty()

# 4. Boucle infinie pour le Temps Réel
while True:
    # On exécute une requête SQL pour lire la table des alertes
    lignes = session.execute("SELECT * FROM alertes")
    
    # On transforme le résultat en tableau de données (DataFrame Pandas)
    df = pd.DataFrame(list(lignes))
    
    # On injecte les données dans la page Web
    with placeholder.container():
        if not df.empty:
            # On renomme les colonnes pour que ce soit plus joli
            df.columns = ["Type d'Attaque", "Nombre de Paquets Bloqués"]
            
            # Affichage de métriques en gros en haut de page
            total_attaques = df["Nombre de Paquets Bloqués"].sum()
            st.metric(label=" TOTAL DES REQUÊTES MALVEILLANTES BLOQUÉES", value=total_attaques)
            
            # Affichage en deux colonnes
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("###  Détail des alertes")
                st.dataframe(df, use_container_width=True)
                
            with col2:
                st.write("###  Répartition des menaces")
                # Un graphique en barres généré automatiquement
                st.bar_chart(data=df, x="Type d'Attaque", y="Nombre de Paquets Bloqués")
                
        else:
            st.info(" Aucun trafic malveillant détecté pour le moment. En attente de données...")
            
    # Le dashboard fait une pause de 2 secondes avant de re-vérifier la base de données
    time.sleep(2)