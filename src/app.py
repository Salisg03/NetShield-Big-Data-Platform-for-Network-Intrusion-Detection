import streamlit as st
import pandas as pd
import time
from cassandra.cluster import Cluster

st.set_page_config(page_title="NetShield SOC Dashboard", layout="wide", page_icon="🛡️")
st.title("🛡️ NetShield - Centre d'Opérations de Cybersécurité")

@st.cache_resource
def get_cassandra_session():
    cluster = Cluster(['localhost'], port=9042)
    return cluster.connect('netshield')

session = get_cassandra_session()
placeholder = st.empty()

while True:
    # On lit le bon schéma : label ET destination_port !
    lignes = session.execute("SELECT label, destination_port, count FROM alertes")
    df = pd.DataFrame(list(lignes))
    
    with placeholder.container():
        if not df.empty:
            df_normal = df[df["label"] == "BENIGN"]
            df_attaques = df[df["label"] != "BENIGN"]
            
            vol_normal = df_normal["count"].sum() if not df_normal.empty else 0
            vol_attaques = df_attaques["count"].sum() if not df_attaques.empty else 0
            
            st.subheader(" Vue d'ensemble du Trafic")
            col1, col2, col3 = st.columns(3)
            
            col1.metric(label=" Paquets Sains (BENIGN)", value=int(vol_normal))
            col2.metric(label=" Total Attaques Bloquées", value=int(vol_attaques))
            
            # On compte les ports différents qui se font attaquer
            nb_ports = df_attaques["destination_port"].nunique() if not df_attaques.empty else 0
            col3.metric(label=" Ports Réseaux Ciblés", value=nb_ports)
            
            st.markdown("---")
            st.subheader("🔍 Analyse des Menaces")
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                st.markdown("**Types d'attaques détectées**")
                if not df_attaques.empty:
                    attaques_par_type = df_attaques.groupby("label")["count"].sum().reset_index()
                    st.bar_chart(data=attaques_par_type.set_index("label"))
                else:
                    st.success("Aucune attaque en cours.")
                    
            with col_chart2:
                st.markdown("** TOP 5 des Ports les plus attaqués**")
                if not df_attaques.empty:
                    top_ports = df_attaques.sort_values(by="count", ascending=False).head(5)
                    st.dataframe(top_ports, hide_index=True) 
                else:
                    st.success("Aucun port ciblé.")
                    
        else:
            with st.spinner("📡 En attente du moteur Spark..."):
                time.sleep(2)
                
    time.sleep(2)