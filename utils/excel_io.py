"""
Lecture Excel vers Polars sans fastexcel (compatibilité Streamlit Cloud / Python 3.14).

Polars read_excel exige fastexcel, souvent indisponible pour certaines versions Python.
openpyxl (.xlsx) et xlrd (.xls) sont déjà dans requirements.txt.
"""
import datetime
import io
from typing import BinaryIO, Union

import numpy as np
import pandas as pd
import polars as pl


def _column_name_suggests_date(name: str) -> bool:
    """N'applique pd.to_datetime qu'aux colonnes clairement liées aux dates (évite Police, montants, etc.)."""
    n = str(name).lower().replace("é", "e").replace("è", "e").replace(" ", "")
    return any(
        k in n
        for k in (
            "effet",
            "echeance",
            "dateeffet",
            "dateecheance",
            "datedebut",
            "datefin",
            "souscription",
        )
    )


def _sanitize_pandas_for_polars(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Évite ArrowTypeError : colonnes object avec types mixtes (int/str/bytes) font échouer
    pl.from_pandas → PyArrow. On homogénéise avant conversion.
    """
    out = pdf.copy()
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_datetime64_any_dtype(s.dtype):
            continue
        if pd.api.types.is_bool_dtype(s.dtype):
            continue
        if pd.api.types.is_numeric_dtype(s.dtype):
            continue
        if isinstance(s.dtype, pd.CategoricalDtype):
            out[col] = s.astype(str).replace("nan", pd.NA)
            continue
        if s.dtype != object and str(s.dtype) != "object":
            continue

        # Colonnes object : datetime seulement si le nom ressemble à Effet / Echeance / …
        if _column_name_suggests_date(str(col)):
            try:
                as_dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
                nn = as_dt.notna().sum()
                if nn > 0 and nn >= 0.4 * len(s):
                    out[col] = as_dt
                    continue
            except Exception:
                pass

        def cell_to_str(x):
            if x is None:
                return ""
            if isinstance(x, float) and np.isnan(x):
                return ""
            if isinstance(x, bytes):
                return x.decode("utf-8", errors="replace")
            if isinstance(x, (datetime.datetime, datetime.date, pd.Timestamp)):
                return x.isoformat() if hasattr(x, "isoformat") else str(x)
            return str(x)

        out[col] = [cell_to_str(v) for v in s.tolist()]

    return out


def read_excel_to_polars(
    source: Union[bytes, BinaryIO],
    filename: str,
    sheet_name: str | int = 0,
    *,
    string_cells: bool = False,
) -> pl.DataFrame:
    """
    Lit une feuille Excel et retourne un DataFrame Polars.

    Args:
        source: Contenu du fichier (bytes) ou buffer BytesIO.
        filename: Nom du fichier (pour choisir openpyxl vs xlrd).
        sheet_name: Nom ou index de feuille (comme pandas).
        string_cells: Si True, lit toutes les colonnes en str (recommandé pour fichiers sinistres avec tirets).
    """
    if isinstance(source, bytes):
        buf = io.BytesIO(source)
    else:
        buf = source
        buf.seek(0)

    fn = (filename or "").lower()
    if fn.endswith(".xls") and not fn.endswith(".xlsx"):
        engine = "xlrd"
    else:
        engine = "openpyxl"

    read_kw: dict = {}
    if string_cells:
        # Toutes les cellules en texte : évite les inférences int64 sur des "-" / "- " / vides Excel
        read_kw["dtype"] = str

    pdf = pd.read_excel(buf, sheet_name=sheet_name, engine=engine, **read_kw)
    if not string_cells:
        pdf = _sanitize_pandas_for_polars(pdf)
    return pl.from_pandas(pdf)
