"""
Configuration et constantes de l'application
Optimisé pour le traitement de gros volumes (500K+ lignes)
"""
from datetime import datetime
from pathlib import Path

# =============================================================================
# CHEMINS
# =============================================================================
DATA_DIR = Path("data")
PARQUET_DIR = DATA_DIR / "parquet"
CACHE_DIR = DATA_DIR / "cache"

# Créer les répertoires s'ils n'existent pas
DATA_DIR.mkdir(exist_ok=True)
PARQUET_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# =============================================================================
# CONFIGURATION GÉNÉRALE
# =============================================================================
APP_TITLE = "Dashboard Actuariat - PREC/PA"
APP_LAYOUT = "wide"

# =============================================================================
# BRANCHES CIBLES PRÉDÉFINIES
# =============================================================================
BRANCHES_CIBLES = [
    "AUTOMOBILE",
    "AUTRES DOMMAGES AUX BIENS",
    "CAUTION",
    "DOMMAGES CORPORELS",
    "INCENDIE & MULTIRISQUES",
    "RESPONSABILITE CIVILE",
    "RISQUE AGRICOLE",
    "SANTE",
    "TRANSPORT",
    "VOYAGE"
]

# =============================================================================
# VALEURS PAR DÉFAUT
# =============================================================================
DEFAULT_DATE_EVAL = datetime(2025, 12, 31)
DEFAULT_FRAIS_GENERAUX = 2_500_000.0
DEFAULT_LOSS_RATIO = 0.65
TAUX_PREC_MIN = 0.72

# =============================================================================
# SCHÉMA DES DONNÉES (types optimisés pour Polars)
# =============================================================================
SCHEMA_PRODUCTION = {
    "Exercice": "str",
    "Police": "str",
    "Client": "str",
    "Branche": "str",
    "Categorie": "str",
    "Intermediaire": "str",
    "Prime_Nette": "float64",
    "Accessoires": "float64",
    "Effet": "date",
    "Echeance": "date"
}

COLONNES_PRODUCTION = list(SCHEMA_PRODUCTION.keys())
COLONNES_REGLEMENTS = ["Branche", "Exercice", "Montant"]

# =============================================================================
# CONFIGURATION PARQUET (optimisation compression/vitesse)
# =============================================================================
PARQUET_COMPRESSION = "zstd"  # Meilleur ratio compression/vitesse
PARQUET_ROW_GROUP_SIZE = 100_000  # Optimal pour 500K lignes
