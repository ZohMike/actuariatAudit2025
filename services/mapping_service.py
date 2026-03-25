"""
Service de mapping des branches
"""
import streamlit as st
from config import BRANCHES_CIBLES


class MappingService:
    """Service pour la gestion du mapping des branches"""
    
    @staticmethod
    def get_branches_cibles() -> list[str]:
        """Retourne la liste des branches cibles prédéfinies"""
        return BRANCHES_CIBLES.copy()
    
    @staticmethod
    def find_default_mapping_index(branche_obs: str, branches_cibles: list[str]) -> int:
        """Trouve l'index par défaut pour le mapping."""
        branche_upper = branche_obs.upper().strip()
        for idx, bc in enumerate(branches_cibles):
            if bc.upper() == branche_upper:
                return idx
        return 0
    
    @staticmethod
    def save_mapping(mapping: dict) -> None:
        """Sauvegarde le mapping dans le session_state"""
        st.session_state['mapping_valide'] = mapping
    
    @staticmethod
    def get_saved_mapping() -> dict | None:
        """Récupère le mapping sauvegardé"""
        return st.session_state.get('mapping_valide')
    
    @staticmethod
    def is_mapping_validated() -> bool:
        """Vérifie si le mapping a été validé"""
        return 'mapping_valide' in st.session_state

    @staticmethod
    def invalidate_mapping() -> None:
        """Invalide le mapping actuel"""
        if 'mapping_valide' in st.session_state:
            del st.session_state['mapping_valide']
