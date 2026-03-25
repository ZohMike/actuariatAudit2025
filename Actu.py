"""
POINT D'ENTRÉE LEGACY - Redirige vers app.py

Pour lancer l'application optimisée:
    streamlit run app.py

Architecture optimisée pour 500K+ lignes:
- Conversion one-shot Excel → Parquet
- LazyFrame (scan_parquet) pour les calculs
- Cache Streamlit intelligent
"""
print("""
╔══════════════════════════════════════════════════════════════╗
║           Dashboard Actuariat - PREC/PA                      ║
╠══════════════════════════════════════════════════════════════╣
║  Pour lancer l'application optimisée:                        ║
║                                                              ║
║    streamlit run app.py                                      ║
║                                                              ║
║  Optimisations:                                              ║
║  • Conversion Excel → Parquet (100x plus rapide)             ║
║  • LazyFrame Polars (évaluation différée)                    ║
║  • Cache intelligent (pas de recalcul inutile)               ║
║  • Support 500K+ lignes                                      ║
╚══════════════════════════════════════════════════════════════╝
""")

import subprocess
import sys

if __name__ == "__main__":
    subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])
