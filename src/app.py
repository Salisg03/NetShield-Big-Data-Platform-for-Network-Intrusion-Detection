import streamlit as st
import pandas as pd
import time
from cassandra.cluster import Cluster
import datetime

st.set_page_config(page_title="NetShield SOC", layout="wide", page_icon="🛡️")
st.title("🛡️ NetShield - Centre d'Opérations de Cybersécurité (SOC)")

# Dictionnaire de traduction des ports
PORT_SERVICES = {
    "80": "HTTP (Web)", "443": "HTTPS (Web Sécurisé)", "22": "SSH (Accès distant)",
    "21": "FTP (Fichiers)", "53": "DNS (Domaines)", "3389": "RDP (Bureau à distance)",
    "445": "SMB (Partage Windows)", "8080": "HTTP Alternatif", "139": "NetBIOS"
}

@st.cache_resource
def get_cassandra_session():
    cluster = Cluster(['localhost'], port=9042)
    return cluster.connect('netshield')

session = get_cassandra_session()

# Initialisation de la mémoire du Dashboard pour dessiner des courbes en direct
if 'historique_courbe' not in st.session_state:
    st.session_state['historique_courbe'] = pd.DataFrame(columns=['Heure', 'Trafic Sain', 'Attaques'])
if 'derniers_comptes' not in st.session_state:
    st.session_state['derniers_comptes'] = {'sain': 0, 'attaques': 0}

tab1, tab2 = st.tabs([" Surveillance Temps Réel (Speed Layer)", " Historique HDFS (Batch Layer)"])

with tab1:
    placeholder_rt = st.empty()
with tab2:
    placeholder_batch = st.empty()

while True:
    # --- LECTURE CASSANDRA ---
    lignes_rt = session.execute("SELECT label, destination_port, count FROM alertes")
    df_rt = pd.DataFrame(list(lignes_rt))
    
    try:
        lignes_hist = session.execute("SELECT label, destination_port, count FROM historique")
        df_hist = pd.DataFrame(list(lignes_hist))
    except:
        df_hist = pd.DataFrame()

    # =========================================================
    # ONGLET 1 : TEMPS RÉEL
    # =========================================================
    with placeholder_rt.container():
        if not df_rt.empty:
            df_normal = df_rt[df_rt["label"] == "BENIGN"].copy()
            df_attaques = df_rt[df_rt["label"] != "BENIGN"].copy()
            
            vol_normal = df_normal["count"].sum() if not df_normal.empty else 0
            vol_attaques = df_attaques["count"].sum() if not df_attaques.empty else 0
            total_trafic = vol_normal + vol_attaques
            
            # Calcul de la vitesse (Delta = Nouveau total - Ancien total)
            delta_normal = vol_normal - st.session_state['derniers_comptes']['sain']
            delta_attaques = vol_attaques - st.session_state['derniers_comptes']['attaques']
            
            # Mise à jour de la mémoire
            st.session_state['derniers_comptes']['sain'] = vol_normal
            st.session_state['derniers_comptes']['attaques'] = vol_attaques
            
            # Ajout des données à la courbe temporelle
            nouvelle_ligne = pd.DataFrame({
                'Heure': [datetime.datetime.now().strftime("%H:%M:%S")],
                'Trafic Sain': [vol_normal],
                'Attaques': [vol_attaques]
            })
            st.session_state['historique_courbe'] = pd.concat([st.session_state['historique_courbe'], nouvelle_ligne]).tail(30) # Garde les 30 derniers points
            
            taux_menace = (vol_attaques / total_trafic * 100) if total_trafic > 0 else 0
            
            # 1. Bannières et Jauge de menace
            if taux_menace > 20:
                st.error(f"🚨 ALERTE CRITIQUE : Cyberattaque en cours ! ({taux_menace:.1f}% du flux)")
                st.progress(taux_menace / 100, text="Niveau de criticité")
            elif taux_menace > 0:
                st.warning(f"⚠️ AVERTISSEMENT : Activité suspecte détectée. ({taux_menace:.1f}%)")
            else:
                st.success("✅ État du réseau : SÉCURISÉ.")
            
            # 2. Métriques (Avec les flèches Delta dynamiques !)
            st.markdown("### 📊 Statistiques de Flux")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Trafic", int(total_trafic))
            col2.metric("🟢 Trafic Sain", int(vol_normal), delta=f"{int(delta_normal)} paquets/sec", delta_color="normal")
            col3.metric("🔴 Attaques Bloquées", int(vol_attaques), delta=f"{int(delta_attaques)} paquets/sec", delta_color="inverse")
            nb_ports = df_attaques["destination_port"].nunique() if not df_attaques.empty else 0
            col4.metric("🔌 Ports Ciblés", int(nb_ports))
            
            st.markdown("---")
            
            # 3. La courbe temporelle (Live Line Chart)
            st.markdown("#### 📈 Évolution du Trafic en direct")
            st.line_chart(data=st.session_state['historique_courbe'].set_index('Heure'), color=["#00FF00", "#FF0000"])
            
            st.markdown("---")
            
            # 4. Graphique Barres et Tableau des Ports
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                st.markdown("####  Répartition par types d'attaques")
                if not df_attaques.empty:
                    attaques_grp = df_attaques.groupby("label")["count"].sum().reset_index()
                    st.bar_chart(data=attaques_grp.set_index("label"))
                else:
                    st.info("Aucune attaque en cours.")
                    
            with col_chart2:
                st.markdown("####  TOP 5 des Services les plus ciblés")
                if not df_attaques.empty:
                    df_attaques["Service Cible"] = df_attaques["destination_port"].apply(
                        lambda x: PORT_SERVICES.get(str(x), f"Port Inconnu ({x})")
                    )
                    top_ports = df_attaques.groupby("Service Cible")["count"].sum().reset_index()
                    top_ports = top_ports.sort_values(by="count", ascending=False).head(5)
                    top_ports.columns = ["Service Cible", "Nombre d'Attaques"]
                    st.dataframe(top_ports, hide_index=True)
                else:
                    st.info("Aucun service ciblé.")
        else:
            st.info(" En attente du flux Kafka et du traitement Spark Streaming...")

    # ONGLET 2 : HISTORIQUE BATCH
    with placeholder_batch.container():
        st.markdown("###  Analyse globale des données froides (HDFS Data Lake)")
        if not df_hist.empty:
            df_attaques_hist = df_hist[df_hist["label"] != "BENIGN"]
            vol_attaques_hist = df_attaques_hist["count"].sum() if not df_attaques_hist.empty else 0
            
            st.metric(label="☠️ Total Historique des Attaques", value=int(vol_attaques_hist))
            st.markdown("---")
            
            col_h1, col_h2 = st.columns(2)
            with col_h1:
                st.markdown("**Répartition Historique des Menaces**")
                if not df_attaques_hist.empty:
                    hist_grp = df_attaques_hist.groupby("label")["count"].sum().reset_index()
                    st.bar_chart(data=hist_grp.set_index("label"))
            with col_h2:
                st.markdown("**Top 10 Global des Ports Ciblés**")
                if not df_attaques_hist.empty:
                    top_ports_hist = df_attaques_hist.groupby("destination_port")["count"].sum().reset_index()
                    top_ports_hist = top_ports_hist.sort_values(by="count", ascending=False).head(10)
                    st.dataframe(top_ports_hist, hide_index=True)
        else:
            st.warning(" Le Job Batch analyse actuellement les fichiers dans HDFS. Veuillez patienter...")

    time.sleep(2)