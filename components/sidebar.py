"""
Composant Sidebar
"""
import streamlit as st
from datetime import datetime
from config import DEFAULT_DATE_EVAL, DEFAULT_FRAIS_GENERAUX
from utils.templates import create_production_template


def render_sidebar() -> tuple:
    """Affiche la sidebar avec les paramètres et l'upload des fichiers."""
    with st.sidebar:
        st.header("⚙️ Paramètres")
        
        date_eval = st.date_input(
            "Date d'évaluation",
            DEFAULT_DATE_EVAL
        )
        
        frais_generaux = st.number_input(
            "Total Frais Généraux",
            min_value=0.0,
            value=DEFAULT_FRAIS_GENERAUX,
            format="%.2f"
        )
        
        st.divider()
        
        st.header("📄 Template")
        st.download_button(
            label="📥 Template Production",
            data=create_production_template(),
            file_name="template_production.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        st.divider()
        if st.button("🔄 Vider le cache", use_container_width=True, type="secondary"):
            st.cache_data.clear()
            from config import PARQUET_DIR
            import shutil
            if PARQUET_DIR.exists():
                shutil.rmtree(PARQUET_DIR)
                PARQUET_DIR.mkdir(exist_ok=True)
            st.rerun()
    
    return date_eval, frais_generaux


def display_data_stats(stats: dict) -> None:
    """Affiche les statistiques des données dans la sidebar."""
    with st.sidebar:
        st.divider()
        st.header("📊 Données")
        col1, col2 = st.columns(2)
        col1.metric("Lignes", f"{stats['rows']:,}")
        col2.metric("Colonnes", stats['columns'])
