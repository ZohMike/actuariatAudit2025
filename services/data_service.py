"""
Service de gestion des données - Optimisé pour gros volumes (500K+ lignes)
"""
import streamlit as st
import polars as pl
import hashlib
import io
from pathlib import Path
from typing import Optional

from config import (
    PARQUET_DIR,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE
)
from utils.excel_io import read_excel_to_polars


class DataService:
    """Service optimisé pour le chargement et la transformation des données."""
    
    @staticmethod
    def compute_files_hash(file_contents: list[tuple[str, str, bytes]]) -> str:
        """Calcule un hash unique pour un ensemble de fichiers (incluant l'année)."""
        hasher = hashlib.md5()
        # Content structure: (year, filename, content)
        for year, filename, content in sorted(file_contents):
            hasher.update(year.encode())
            hasher.update(filename.encode())
            hasher.update(content)
        return hasher.hexdigest()[:16]
    
    @staticmethod
    def get_parquet_path(files_hash: str) -> Path:
        """Retourne le chemin du fichier Parquet pour un hash donné"""
        # Mise à jour v4 pour structure par année
        return PARQUET_DIR / f"production_v4_{files_hash}.parquet"
    
    @staticmethod
    def convert_excel_to_parquet(
        file_contents: list[tuple[str, str, bytes]],
        files_hash: str
    ) -> str:
        """Convertit les fichiers Excel en Parquet."""
        parquet_path = DataService.get_parquet_path(files_hash)
        
        # Si le Parquet existe déjà, ne pas reconvertir
        if parquet_path.exists():
            return str(parquet_path)
        
        return DataService._do_conversion(file_contents, files_hash)
    
    @staticmethod
    @st.cache_data(ttl=3600, show_spinner="Conversion Excel → Parquet...")
    def _do_conversion(
        file_contents: list[tuple[str, str, bytes]],
        files_hash: str
    ) -> str:
        """Fonction interne cachée pour la conversion."""
        parquet_path = DataService.get_parquet_path(files_hash)
        
        dfs = []
        for year, filename, content in file_contents:
            buffer = io.BytesIO(content)
            
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                df = read_excel_to_polars(content, filename, sheet_name=0)
            else:
                df = pl.read_csv(buffer)
            
            df = DataService._normalize_schema(df)
            
            # CRITICAL: Forcer l'exercice avec l'année du slot d'upload
            if "Exercice" in df.columns:
                df = df.drop("Exercice")
                
            df = df.with_columns(
                pl.lit(year).cast(pl.Utf8).alias("Exercice")
            )
            
            dfs.append(df)
        
        combined_df = pl.concat(dfs, how="diagonal_relaxed")
        
        combined_df.write_parquet(
            parquet_path,
            compression=PARQUET_COMPRESSION,
            row_group_size=PARQUET_ROW_GROUP_SIZE,
            statistics=True
        )
        
        return str(parquet_path)
    
    @staticmethod
    def _normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
        """Normalise le schéma d'un DataFrame."""
        transformations = []
        
        # Nettoyage et typage de Branche (uniquement si pas déjà string)
        if "Branche" in df.columns:
            col_dtype = df["Branche"].dtype
            if col_dtype != pl.Utf8:
                transformations.append(
                    pl.col("Branche").cast(pl.Utf8).str.strip_chars()
                )
            
        # Nettoyage et typage de Exercice (uniquement si pas déjà string)
        if "Exercice" in df.columns:
            col_dtype = df["Exercice"].dtype
            if col_dtype != pl.Utf8:
                transformations.append(
                    pl.col("Exercice").cast(pl.Utf8)
                    .str.replace(r"\.0$", "")  # Enlève .0 à la fin (ex: 2023.0 -> 2023)
                    .str.strip_chars()
                )

        # Colonnes numériques (uniquement si pas déjà float)
        if "PN_ACC" in df.columns:
            if df["PN_ACC"].dtype != pl.Float64:
                try:
                    transformations.append(pl.col("PN_ACC").cast(pl.Float64))
                except Exception:
                    pass  # Ignore si conversion impossible
        if "Prime_Nette" in df.columns:
            if df["Prime_Nette"].dtype != pl.Float64:
                try:
                    transformations.append(pl.col("Prime_Nette").cast(pl.Float64))
                except Exception:
                    pass
        if "Accessoires" in df.columns:
            if df["Accessoires"].dtype != pl.Float64:
                try:
                    transformations.append(pl.col("Accessoires").cast(pl.Float64))
                except Exception:
                    pass
        
        if transformations:
            try:
                df = df.with_columns(transformations)
            except Exception as e:
                import streamlit as st
                st.warning(f"Erreur normalisation schema: {e}")
        
        return df
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_unique_branches(_parquet_path: str) -> list[str]:
        """Récupère les branches uniques."""
        branches = pl.read_parquet(_parquet_path)["Branche"].unique().to_list()
        return sorted([b for b in branches if b is not None])
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_unique_exercices(_parquet_path: str) -> list[str]:
        """Récupère les exercices uniques (sans null)."""
        exercices = pl.read_parquet(_parquet_path).select(
            pl.col("Exercice").fill_null("NR")
        )["Exercice"].unique().to_list()
        return sorted([e for e in exercices if e is not None])
    
    @staticmethod
    def get_last_exercice(parquet_path: str) -> str:
        """Retourne le dernier exercice"""
        return DataService.get_unique_exercices(parquet_path)[-1]
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_file_stats(_parquet_path: str) -> dict:
        """Récupère les statistiques du fichier."""
        df = pl.read_parquet(_parquet_path)
        return {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns
        }
    
    @staticmethod
    @st.cache_data(ttl=3600)
    def load_reglements(content: bytes, filename: str) -> pl.DataFrame:
        """Charge le fichier des règlements."""
        buffer = io.BytesIO(content)
        df = pl.read_csv(buffer)
        
        return df.pivot(
            values="Montant",
            index="Branche",
            on="Exercice",
            aggregate_function="sum"
        ).fill_null(0)
    
    @staticmethod
    def prepare_file_contents(files_by_year: dict) -> list[tuple[str, str, bytes]]:
        """Prépare les fichiers uploadés pour le traitement (avec année)."""
        contents = []
        for year, files in files_by_year.items():
            for f in files:
                contents.append((year, f.name, f.read()))
                f.seek(0)
        return contents
    
    @staticmethod
    def cleanup_old_parquet_files(keep_hash: str = None, max_files: int = 5):
        """Nettoie les anciens fichiers Parquet."""
        parquet_files = sorted(
            PARQUET_DIR.glob("production_v4_*.parquet"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        for pf in parquet_files[max_files:]:
            if keep_hash and keep_hash in pf.name:
                continue
            pf.unlink()
