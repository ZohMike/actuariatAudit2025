"""
Composant Formulaire de Mapping des Branches
"""
import streamlit as st
import pandas as pd
from services.mapping_service import MappingService


def render_mapping_form(branches_observees: list[str]) -> dict:
    """Affiche le formulaire de mapping des branches."""
    st.header("📌 Étape 0: Mapping des Branches")
    
    branches_cibles = MappingService.get_branches_cibles()
    
    with st.form("mapping_form"):
        st.info("Affectez chaque branche observée à une branche cible.")
        
        mapping_branches = {}
        nb_cols = min(len(branches_observees), 3)
        cols = st.columns(nb_cols) if nb_cols > 0 else [st]
        
        for i, branche_obs in enumerate(branches_observees):
            with cols[i % nb_cols]:
                default_idx = MappingService.find_default_mapping_index(
                    branche_obs, 
                    branches_cibles
                )
                
                mapping_branches[branche_obs] = st.selectbox(
                    f"📌 {branche_obs}",
                    options=branches_cibles,
                    index=default_idx,
                    key=f"map_{branche_obs}"
                )
        
        # Détection de changement de structure (nouvelles/anciennes branches)
        saved_mapping = MappingService.get_saved_mapping()
        if saved_mapping is not None:
            if set(saved_mapping.keys()) != set(mapping_branches.keys()):
                MappingService.invalidate_mapping()
                st.warning("⚠️ Modifications détectées (nouveaux fichiers/branches). Veuillez valider le mapping.")
        
        submitted = st.form_submit_button(
            "✅ Valider le mapping",
            type="primary",
            width="stretch"
        )
    
    if submitted:
        MappingService.save_mapping(mapping_branches)
        st.success("✅ Mapping validé!")
    
    # On retourne toujours ce qu'il y a à l'écran
    return mapping_branches
