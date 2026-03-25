"""
Génération des templates de fichiers
"""
import streamlit as st
import pandas as pd
import io
from datetime import datetime


@st.cache_data
def create_production_template() -> bytes:
    """Crée un template Excel pour la base de production."""
    template_data = {
        "Exercice": [2024, 2024, 2024, 2024],
        "Police": ["POL001", "POL002", "POL003", "POL004"],
        "Client": ["Client A", "Client B", "Client C", "Client D"],
        "Branche": ["AUTO", "INCENDIE", "SANTE", "TRANSPORT"],
        "Categorie": ["Cat1", "Cat2", "Cat1", "Cat3"],
        "Intermediaire": ["Agent 1", "Courtier 2", "Agent 1", "Direct"],
        "Prime_Nette": [100000.0, 50000.0, 75000.0, 30000.0],
        "Accessoires": [5000.0, 2500.0, 3750.0, 1500.0],
        "Effet": [
            datetime(2024, 1, 1),
            datetime(2024, 3, 15),
            datetime(2024, 6, 1),
            datetime(2024, 9, 1)
        ],
        "Echeance": [
            datetime(2025, 1, 1),
            datetime(2025, 3, 15),
            datetime(2025, 6, 1),
            datetime(2025, 9, 1)
        ]
    }
    
    df = pd.DataFrame(template_data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Production')
    
    return output.getvalue()


@st.cache_data
def create_sinistres_template() -> bytes:
    """Crée un template Excel pour la base sinistres."""
    template_data = {
        "BRANCHE": [
            "AUTOMOBILE", "AUTOMOBILE", "AUTOMOBILE",
            "INCENDIES", "INCENDIES",
            "TRANSPORT", "TRANSPORT",
            "SANTE"
        ],
        "GARANTIE": [
            "RC", "Dommages", "Vol",
            "Incendie", "Dégâts des eaux",
            "Marchandises", "Corps",
            "Maladie"
        ],
        "REGLEMENT": [
            500000.0, 250000.0, 100000.0,
            1000000.0, 150000.0,
            300000.0, 200000.0,
            450000.0
        ],
        "COUT_TOTAL": [
            750000.0, 400000.0, 150000.0,
            1500000.0, 200000.0,
            500000.0, 350000.0,
            600000.0
        ]
    }
    
    df = pd.DataFrame(template_data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sinistres')
    
    return output.getvalue()


@st.cache_data
def create_psap_template(years: list) -> bytes:
    """Crée un template Excel pour la saisie manuelle PSAP."""
    # Colonnes dynamique selon les années disponibles
    columns = ["Branche"] + [str(y) for y in years]
    
    # Données par défaut : uniquement SANTE (Dommages Corporels calculé automatiquement via SAP + IBNR)
    data = []
    row = {"Branche": "SANTE"}
    for y in years:
        row[str(y)] = 0.0
    data.append(row)
        
    df = pd.DataFrame(data, columns=columns)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='PSAP_Manuel')
    
    return output.getvalue()
