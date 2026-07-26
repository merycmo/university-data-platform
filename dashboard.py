import streamlit as st
import psycopg2
import pandas as pd

st.set_page_config(page_title="University Data Architecture Dashboard", layout="wide")

def get_connection():
    # Connexion avec encodage adapte aux accents Windows/PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="metastore",
        user="hive",
        password="hive123"
    )
    conn.set_client_encoding('LATIN1')
    return conn

try:
    conn = get_connection()
    df_courses = pd.read_sql("SELECT * FROM course_catalog;", conn)
    df_faculty = pd.read_sql("SELECT * FROM faculty_profiles;", conn)
    conn.close()
    st.success("Connexion reussie a PostgreSQL !")
except Exception as e:
    st.error(f"Erreur de connexion : {str(e).encode('ascii', 'ignore').decode('ascii')}")
    df_courses = pd.DataFrame()
    df_faculty = pd.DataFrame()

# En-tete
st.markdown("""
    <div style="background-color:#1E40AF; padding:25px; border-radius:12px; color:white; margin-bottom:25px;">
        <h1 style="color:white; margin:0;">University Data Architecture Dashboard</h1>
        <p style="margin:5px 0 0 0; opacity:0.9;">Operational Analytics & Metadata Intelligence Platform</p>
    </div>
""", unsafe_allow_html=True)

# Indicateurs principaux
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Courses", len(df_courses))

with col2:
    st.metric("Faculty Members", len(df_faculty))

with col3:
    total_works = int(df_faculty['works_count'].sum()) if not df_faculty.empty and 'works_count' in df_faculty else 0
    st.metric("Total Publications", f"{total_works:,}")

with col4:
    total_citations = int(df_faculty['cited_by_count'].sum()) if not df_faculty.empty and 'cited_by_count' in df_faculty else 0
    st.metric("Total Citations", f"{total_citations:,}")

st.markdown("---")

# Visualisations
c1, c2 = st.columns(2)

with c1:
    st.subheader("Catalogue des Cours")
    if not df_courses.empty:
        st.dataframe(df_courses[['course_name', 'university', 'faculty']], use_container_width=True)
    else:
        st.info("Aucun cours trouve.")

with c2:
    st.subheader("Profils des Enseignants")
    if not df_faculty.empty:
        st.dataframe(df_faculty[['full_name', 'works_count', 'cited_by_count']], use_container_width=True)
    else:
        st.info("Aucun profil trouve.")