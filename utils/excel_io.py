"""
Lecture Excel vers Polars sans fastexcel (compatibilité Streamlit Cloud / Python 3.14).

Polars read_excel exige fastexcel, souvent indisponible pour certaines versions Python.
openpyxl (.xlsx) et xlrd (.xls) sont déjà dans requirements.txt.
"""
import io
from typing import BinaryIO, Union

import pandas as pd
import polars as pl


def read_excel_to_polars(
    source: Union[bytes, BinaryIO],
    filename: str,
    sheet_name: str | int = 0,
) -> pl.DataFrame:
    """
    Lit une feuille Excel et retourne un DataFrame Polars.

    Args:
        source: Contenu du fichier (bytes) ou buffer BytesIO.
        filename: Nom du fichier (pour choisir openpyxl vs xlrd).
        sheet_name: Nom ou index de feuille (comme pandas).
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

    pdf = pd.read_excel(buf, sheet_name=sheet_name, engine=engine)
    return pl.from_pandas(pdf)
