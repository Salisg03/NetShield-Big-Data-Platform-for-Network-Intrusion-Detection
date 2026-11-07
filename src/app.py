import streamlit as st
import pandas as pd
import time
from cassandra.cluster import Cluster

st.set_page_config(page_title="NetShield Dashboard", layout="wide")
st.title(" NetShield - Moniteur de Trafic Réseau en Direct")

# Connexion à Cassandra
@st.cache_resource
def get_cassandra_session():
    cluster = Cluster(['localhost'], port=9042)
    return cluster.connect('netshield')

session = get_cassandra_session()
placeholder = st.empty()

while True:
    lignes = session.execute("SELECT * FROM alertes")
    df = pd.DataFrame(list(lignes))
    
    with placeholder.container():
        if not df.empty:
            df.columns = ["Type de Trafic", "Nombre de Paquets"]
            
            # On sépare le trafic normal des attaques pour l'affichage
            df_normal = df[df["Type de Trafic"] == "BENIGN"]
            df_attaques = df[df["Type de Trafic"] != "BENIGN"]
            
            vol_normal = df_normal["Nombre de Paquets"].sum() if not df_normal.empty else 0
            vol_attaques = df_attaques["Nombre de Paquets"].sum() if not df_attaques.empty else 0
            
            # --- LES DEUX GROS COMPTEURS EN HAUT ---
            col_m1, col_m2 = st.columns(2)
            col_m1.metric(label=" Trafic Sain Analysé (BENIGN)", value=int(vol_normal))
            col_m2.metric(label=" Menaces Bloquées (DDoS / PortScan)", value=int(vol_attaques))
            
            st.markdown("---")
            
            # --- LES GRAPHIQUES ---
            col1, col2 = st.columns(2)
            with col1:
                st.write("###  Tableau de bord brut")
                st.dataframe(df, use_container_width=True)
            with col2:
                st.write("###  Répartition du Réseau")
                st.bar_chart(data=df, x="Type de Trafic", y="Nombre de Paquets")
        else:
            with st.spinner(" Connexion au cluster... En attente des premiers paquets réseaux..."):
                time.sleep(2)
                
    time.sleep(2)