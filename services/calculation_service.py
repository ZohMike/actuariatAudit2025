"""
Service de calculs actuariels - Optimisé avec Polars
"""
import streamlit as st
import polars as pl
import io
from datetime import date
from typing import Tuple

from config import TAUX_PREC_MIN


class CalculationService:
    """Service pour tous les calculs actuariels."""
    
    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def compute_recap_pe(parquet_path: str, mapping: tuple) -> pl.DataFrame:
        """Calcule le tableau récapitulatif PE par Branche et Exercice."""
        mapping_dict = dict(mapping)
        
        df = pl.read_parquet(parquet_path)
        
        # Remplacer les Exercice null par "NR"
        df = df.with_columns(pl.col("Exercice").fill_null("NR"))
        
        # Remplacer les nulls par 0
        df = df.with_columns([
            pl.col("PN_ACC").fill_null(0)
        ])
        
        # Appliquer le mapping
        df = df.with_columns(
            pl.col("Branche").cast(pl.Utf8).replace_strict(
                mapping_dict, 
                default=pl.col("Branche")
            ).alias("Branche")
        )
        
        # Calculer PE (Utilisation directe de PN_ACC)
        df = df.with_columns(
            pl.col("PN_ACC").alias("PE")
        )
        
        # Agrégation
        result = df.group_by(["Branche", "Exercice"]).agg(
            pl.sum("PE").alias("PE")
        )
        
        pivot_df = result.pivot(
            values="PE",
            index="Branche",
            on="Exercice"
        ).fill_null(0).sort("Branche")
        
        # Trier les colonnes: Branche, exercices triés, Total
        exercice_cols = sorted([col for col in pivot_df.columns if col != "Branche"])
        
        # Ajouter colonne Total
        pivot_df = pivot_df.with_columns(
            pl.sum_horizontal(exercice_cols).alias("Total")
        )
        
        # Réordonner les colonnes
        pivot_df = pivot_df.select(["Branche"] + exercice_cols + ["Total"])
        
        # Ajouter ligne Total
        totals = {"Branche": "TOTAL"}
        for col in exercice_cols + ["Total"]:
            totals[col] = pivot_df[col].sum()
        
        total_row = pl.DataFrame([totals])
        pivot_df = pl.concat([pivot_df, total_row])
        
        return pivot_df
    
    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def compute_taux_prec(
        parquet_path: str,
        mapping: tuple,
        loss_ratios: tuple,
        frais_generaux: float,
        last_exercice: str
    ) -> pl.DataFrame:
        """Calcule les taux PREC par branche."""
        mapping_dict = dict(mapping)
        loss_ratios_dict = dict(loss_ratios)
        
        df = pl.read_parquet(parquet_path)
        
        # Remplacer les Exercice null par "NR"
        df = df.with_columns(pl.col("Exercice").fill_null("NR"))
        
        # Remplacer les nulls par 0
        df = df.with_columns([
            pl.col("PN_ACC").fill_null(0)
        ])
        
        df = df.with_columns(
            pl.col("Branche").cast(pl.Utf8).replace_strict(
                mapping_dict,
                default=pl.col("Branche")
            ).alias("Branche")
        )
        
        pe_last_year = (
            df
            .filter(pl.col("Exercice").cast(pl.Utf8) == last_exercice)
            .with_columns(
                pl.col("PN_ACC").alias("PE")
            )
            .group_by("Branche")
            .agg(pl.sum("PE").alias("PE_Total"))
        )
        
        total_pe = pe_last_year["PE_Total"].sum()
        
        df_lr = pl.DataFrame({
            "Branche": list(loss_ratios_dict.keys()),
            "Loss_Ratio": list(loss_ratios_dict.values())
        })
        
        df_taux = (
            df_lr
            .join(pe_last_year, on="Branche", how="left")
            .fill_null(0)
            .with_columns(
                (pl.col("PE_Total") / total_pe * frais_generaux).alias("FG_Reparti")
            )
            .with_columns(
                # Si PE_Total = 0, on utilise Max(72%, LR) sinon Max(72%, LR + 0.5 * FG / PE)
                pl.when(pl.col("PE_Total") == 0)
                    .then(pl.max_horizontal(TAUX_PREC_MIN, pl.col("Loss_Ratio")))
                    .otherwise(
                        pl.max_horizontal(
                            TAUX_PREC_MIN,
                            pl.col("Loss_Ratio") + (0.5 * pl.col("FG_Reparti") / pl.col("PE_Total"))
                        )
                    )
                    .alias("Taux_PREC")
            )
        )
        
        return df_taux
    
    @staticmethod
    @st.cache_data(ttl=3600, show_spinner="Calcul PREC en cours...")
    def compute_prec(
        parquet_path: str,
        mapping: tuple,
        taux_prec: tuple,
        date_eval: date
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Calcule la PREC ligne à ligne."""
        mapping_dict = dict(mapping)
        taux_dict = dict(taux_prec)
        
        df_taux = pl.DataFrame({
            "Branche": list(taux_dict.keys()),
            "Taux_PREC": list(taux_dict.values())
        })
        
        df = pl.read_parquet(parquet_path)
        
        # Remplacer les Exercice null par "NR"
        df = df.with_columns(pl.col("Exercice").fill_null("NR"))
        
        # Remplacer les nulls par 0
        df = df.with_columns([
            pl.col("PN_ACC").fill_null(0)
        ])
        
        df = df.with_columns(
            pl.col("Branche").cast(pl.Utf8).replace_strict(
                mapping_dict,
                default=pl.col("Branche")
            ).alias("Branche")
        )
        
        df = df.with_columns(
            pl.col("PN_ACC").alias("PE")
        )
        
        df = df.join(df_taux, on="Branche", how="left")
        
        erreurs = df.filter(pl.col("Effet") > pl.col("Echeance")).select(
            ["Police", "Branche", "Effet", "Echeance"]
        )
        
        df = df.with_columns([
            (pl.col("Echeance") - pl.col("Effet")).dt.total_days().alias("duree_totale"),
            pl.when(pl.lit(date_eval) > pl.col("Echeance"))
                .then(0)
                .when(pl.col("Effet") > pl.lit(date_eval))
                .then((pl.col("Echeance") - pl.col("Effet")).dt.total_days())
                .otherwise((pl.col("Echeance") - pl.lit(date_eval)).dt.total_days())
                .alias("duree_restante")
        ])
        
        # Remplacer Taux_PREC null par 0.72 (taux minimum)
        df = df.with_columns(
            pl.col("Taux_PREC").fill_null(TAUX_PREC_MIN)
        )
        
        # Calcul PREC avec gestion division par 0 (duree_totale = 0)
        df = df.with_columns(
            pl.when(pl.col("duree_totale") == 0)
                .then(0)  # Si durée = 0, PREC = 0
                .otherwise(
                    pl.col("PE") * (pl.col("duree_restante") / pl.col("duree_totale")) * pl.col("Taux_PREC")
                )
                .fill_null(0)
                .alias("PREC")
        )
        
        recap_data = df.group_by(["Branche", "Exercice"]).agg(
            pl.sum("PREC").alias("PREC")
        )
        
        recap_prec = recap_data.pivot(
            values="PREC",
            index="Branche",
            on="Exercice"
        ).fill_null(0).sort("Branche")
        
        # Trier les colonnes: Branche, exercices triés, Total
        exercice_cols = sorted([col for col in recap_prec.columns if col != "Branche"])
        
        # Ajouter colonne Total
        recap_prec = recap_prec.with_columns(
            pl.sum_horizontal(exercice_cols).alias("Total")
        )
        
        # Réordonner les colonnes
        recap_prec = recap_prec.select(["Branche"] + exercice_cols + ["Total"])
        
        # Ajouter ligne Total
        totals = {"Branche": "TOTAL"}
        for col in exercice_cols + ["Total"]:
            totals[col] = recap_prec[col].sum()
        
        total_row = pl.DataFrame([totals])
        recap_prec = pl.concat([recap_prec, total_row])
        
        return recap_prec, erreurs
    
    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def compute_pa(
        recap_pe_data: bytes,
        recap_prec_data: bytes
    ) -> pl.DataFrame:
        """Calcule PA = PE - PREC."""
        recap_pa = CalculationService._compute_pa_base(recap_pe_data, recap_prec_data)
        
        # Recalculer la colonne Total
        exercices = [col for col in recap_pa.columns if col not in ["Branche", "Total"]]
        if "Total" in recap_pa.columns:
            recap_pa = recap_pa.drop("Total")
            
        # Ajouter colonne Total
        recap_pa = recap_pa.with_columns(
            pl.sum_horizontal(exercices).alias("Total")
        )
        
        # Réordonner les colonnes
        recap_pa = recap_pa.select(["Branche"] + exercices + ["Total"])
        
        # Ajouter ligne Total
        totals = {"Branche": "TOTAL"}
        for col in exercices + ["Total"]:
            totals[col] = recap_pa[col].sum()
        
        total_row = pl.DataFrame([totals])
        recap_pa = pl.concat([recap_pa, total_row])
        
        return recap_pa

    @staticmethod
    def _compute_pa_base(recap_pe_data: bytes, recap_prec_data: bytes) -> pl.DataFrame:
        """Base calculation for PA before totals."""
        recap_pe = pl.DataFrame.deserialize(io.BytesIO(recap_pe_data), format="binary")
        recap_prec = pl.DataFrame.deserialize(io.BytesIO(recap_prec_data), format="binary")
        
        # Exclure la ligne TOTAL pour le calcul
        recap_pe_calc = recap_pe.filter(pl.col("Branche") != "TOTAL")
        recap_prec_calc = recap_prec.filter(pl.col("Branche") != "TOTAL")
        
        rename_dict = {col: f"{col}_prec" for col in recap_prec_calc.columns if col != "Branche"}
        recap_prec_renamed = recap_prec_calc.rename(rename_dict)
        
        recap_pa = recap_pe_calc.join(recap_prec_renamed, on="Branche", how="left").fill_null(0)
        
        exercices = [col for col in recap_pe_calc.columns if col not in ["Branche", "Total"]]
        for ex in exercices:
            recap_pa = recap_pa.with_columns(
                (pl.col(ex) - pl.col(f"{ex}_prec")).alias(ex)
            ).drop(f"{ex}_prec")
            
        # Trier les exercices
        exercices_sorted = sorted(exercices)
        return recap_pa.select(["Branche"] + exercices_sorted)

    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def compute_pa_split(
        recap_pa_data: bytes,
        dist_plateforme_data: list,
        dist_type_data: list
    ) -> pl.DataFrame:
        """Splits AUTOMOBILE branch into AUTO RC and AUTO AR."""
        recap_pa = pl.DataFrame.deserialize(io.BytesIO(recap_pa_data), format="binary")
        
        # Convert lists to DataFrames/Dicts for easier processing
        # dist_plateforme: [{"Annee": "2022", "Plateforme": 0.0, "Hors": 1.0}, ...]
        # dist_type: [{"Type": "RC", "Plateforme": 0.13, "Hors": 0.18}, ...]
        
        # Create lookup for distributions
        dist_plat_map = {str(d["Annee"]): d for d in dist_plateforme_data}
        
        rc_dist = next((d for d in dist_type_data if d["Type"] == "AUTO RC"), None)
        ar_dist = next((d for d in dist_type_data if d["Type"] == "AUTO AR"), None)
        
        if not rc_dist or not ar_dist:
            return recap_pa
            
        # Filter out TOTAL row first
        df_work = recap_pa.filter(pl.col("Branche") != "TOTAL")
        
        # Get Automobile row
        auto_row = df_work.filter(pl.col("Branche") == "AUTOMOBILE")
        other_rows = df_work.filter(pl.col("Branche") != "AUTOMOBILE")
        
        if auto_row.height == 0:
            return recap_pa
            
        # Calculate split rows
        rc_row = {"Branche": "AUTO RC"}
        ar_row = {"Branche": "AUTO AR"}
        
        exercices = [col for col in df_work.columns if col != "Branche" and col != "Total"]
        
        for ex in exercices:
            val_auto = auto_row[ex].item()
            
            # Get distribution for this year
            # Default to 0/100 if year not found to avoid crash, or use 2025?
            # User provided example 2022-2025.
            d_plat = dist_plat_map.get(str(ex), {"Plateforme": 0.0, "Hors": 0.0}) 
            
            part_plat = d_plat["Plateforme"]
            part_hors = d_plat["Hors"]
            
            # Coeffs
            coeff_rc = (part_plat * rc_dist["Plateforme"]) + (part_hors * rc_dist["Hors"])
            coeff_ar = (part_plat * ar_dist["Plateforme"]) + (part_hors * ar_dist["Hors"])
            
            # Normalize coeffs if they don't sum to 1? Or trust user input?
            # User input: 87+13=100. Assume trust.
            
            rc_row[ex] = val_auto * coeff_rc
            ar_row[ex] = val_auto * coeff_ar
            
        df_rc = pl.DataFrame([rc_row])
        df_ar = pl.DataFrame([ar_row])
        
        # Combine
        # Ensure we don't have Total column in other_rows to match new rows
        if "Total" in other_rows.columns:
            other_rows = other_rows.drop("Total")
            
        df_final = pl.concat([other_rows, df_rc, df_ar]).sort("Branche")
        
        # Recalculate Total Column
        df_final = df_final.with_columns(
             pl.sum_horizontal(exercices).alias("Total")
        )
        
        # Add Total Row
        totals = {"Branche": "TOTAL"}
        columns = exercices + ["Total"]
        for col in columns:
            totals[col] = df_final[col].sum()
            
        df_final = pl.concat([df_final, pl.DataFrame([totals])])
        
        return df_final

    
    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def compute_cu(
        recap_pa_data: bytes,
        loss_ratios: tuple
    ) -> pl.DataFrame:
        """Calcule la Charge à l'Ultime (CU = PA * Loss Ratio)."""
        recap_pa = pl.DataFrame.deserialize(io.BytesIO(recap_pa_data), format="binary")
        loss_ratios_dict = dict(loss_ratios)
        
        df_lr = pl.DataFrame({
            "Branche": list(loss_ratios_dict.keys()),
            "Loss_Ratio": list(loss_ratios_dict.values())
        })
        
        # --- Nettoyage et Préparation ---
        
        # 1. Nettoyer les branches (strip) pour garantir la jointure
        recap_pa = recap_pa.with_columns(pl.col("Branche").str.strip_chars())
        df_lr = df_lr.with_columns(pl.col("Branche").str.strip_chars())
        
        # 2. Exclure TOTAL pour le calcul
        recap_cu = recap_pa.filter(pl.col("Branche") != "TOTAL")
        if "Total" in recap_cu.columns:
            recap_cu = recap_cu.drop("Total")
            
        # 3. Identifier les colonnes années
        years_cols = [col for col in recap_cu.columns if col != "Branche"]
        
        # --- Calcul ---
        # Jointure pour récupérer les Loss Ratios
        recap_cu = recap_cu.join(df_lr, on="Branche", how="left")
        
        # Appliquer le ratio pour chaque année
        for col in years_cols:
            recap_cu = recap_cu.with_columns(
                (pl.col(col) * pl.col("Loss_Ratio").fill_null(0.0)).alias(col)
            )
            
        recap_cu = recap_cu.drop("Loss_Ratio")
        
        # --- Totaux ---
        # 1. Colonne Total
        recap_cu = recap_cu.with_columns(
            pl.sum_horizontal(years_cols).alias("Total")
        )
        
        # 2. Ligne Total
        recap_cu = recap_cu.sort("Branche")
        
        cols_final = ["Branche"] + sorted(years_cols) + ["Total"]
        recap_cu = recap_cu.select(cols_final)
            
        totals = {"Branche": "TOTAL"}
        for col in cols_final[1:]:
            totals[col] = recap_cu[col].sum()
            
        recap_cu = pl.concat([recap_cu, pl.DataFrame([totals])])
        
        return recap_cu
    
    @staticmethod
    def compute_aggregated_cu(recap_cu_data: bytes) -> pl.DataFrame:
        """Calcule une version agrégée de la CU (Regroupe AUTO RC et AUTO AR)."""
        recap_cu = pl.DataFrame.deserialize(io.BytesIO(recap_cu_data), format="binary")
        
        # Filtrer pour séparer Auto et les autres
        df_auto_split = recap_cu.filter(pl.col("Branche").is_in(["AUTO RC", "AUTO AR"]))
        df_others = recap_cu.filter(~pl.col("Branche").is_in(["AUTO RC", "AUTO AR", "TOTAL"]))
        
        if df_auto_split.height == 0:
            return recap_cu
            
        # Agréger Auto
        value_cols = [c for c in recap_cu.columns if c != "Branche"]
        
        df_auto_agg = df_auto_split.select(value_cols).sum()
        df_auto_agg = df_auto_agg.with_columns(pl.lit("AUTOMOBILE").alias("Branche"))
        df_auto_agg = df_auto_agg.select(["Branche"] + value_cols)
        
        # Combiner
        df_final = pl.concat([df_others, df_auto_agg]).sort("Branche")
        
        # Recalculer Total
        # recalculer la colonne Total d'abord si elle existe déjà dans value_cols
        # (normalement oui, compute_cu retourne un DF complet) // Non, compute_cu retourne colonnes années. 
        # Mais le DF passé contient déjà Total? Vérifions. compute_cu retourne recap_pa.clone() ... join.
        # recap_pa contient Total. Donc value_cols contient Total.
        # sum() sur Total fonctionne.
        
        # Recalculer la ligne TOTAL pour être sûr
        totals = {"Branche": "TOTAL"}
        for col in value_cols:
            totals[col] = df_final[col].sum()
            
        df_final = pl.concat([df_final, pl.DataFrame([totals])])
        
        return df_final
    
    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def compute_psap(
        recap_cu_data: bytes,
        df_reg_data: bytes,
        manual_overrides: list = None
    ) -> pl.DataFrame:
        """
        Calcule PSAP = CU agrégé - Règlements.
        1. Agrège CU (somme AUTO RC + AUTO AR -> AUTOMOBILE) pour s'aligner sur les Règlements.
        2. Soustrait les Règlements.
        3. Applique les surcharges manuelles.
        Note: On exclut la colonne 'Total' des calculs intermédiaires pour la recalculer proprement à la fin.
        """
        recap_cu = pl.DataFrame.deserialize(io.BytesIO(recap_cu_data), format="binary")
        df_reg = pl.DataFrame.deserialize(io.BytesIO(df_reg_data), format="binary")
        
        # Supprimer Total des dataframes d'entrée s'il existe pour éviter des confusions
        # On recalculera le Total à la fin
        if "Total" in recap_cu.columns:
            recap_cu = recap_cu.drop("Total")
        if "Total" in df_reg.columns:
            df_reg = df_reg.drop("Total")
            
        # Colonnes valeurs (Années uniquement)
        value_cols = [c for c in recap_cu.columns if c != "Branche"]
        
        # Exclure la ligne TOTAL existante (on la recalculera à la fin)
        recap_cu = recap_cu.filter(pl.col("Branche") != "TOTAL")
        
        # --- Étape 1 : Agrégation de CU ---
        df_auto_split = recap_cu.filter(pl.col("Branche").is_in(["AUTO RC", "AUTO AR"]))
        recap_cu_clean = recap_cu
        
        if df_auto_split.height > 0:
            df_others = recap_cu_clean.filter(~pl.col("Branche").is_in(["AUTO RC", "AUTO AR"]))
            
            df_auto_agg = df_auto_split.select(value_cols).sum()
            df_auto_agg = df_auto_agg.with_columns(pl.lit("AUTOMOBILE").alias("Branche"))
            df_auto_agg = df_auto_agg.select(["Branche"] + value_cols)
            
            recap_cu_aggregated = pl.concat([df_others, df_auto_agg])
        else:
            recap_cu_aggregated = recap_cu_clean

        # --- Étape 2 : Jointure et Soustraction ---
        df_reg = df_reg.filter(pl.col("Branche") != "TOTAL")
        
        # S'assurer que df_reg a toutes les colonnes années nécessaires
        for col in value_cols:
            if col not in df_reg.columns:
                df_reg = df_reg.with_columns(pl.lit(0.0).alias(col))
        
        df_reg_aligned = df_reg.select(["Branche"] + value_cols).fill_null(0)
        
        rename_dict = {col: f"{col}_reg" for col in value_cols}
        df_reg_renamed = df_reg_aligned.rename(rename_dict)
        
        recap_psap = recap_cu_aggregated.join(df_reg_renamed, on="Branche", how="left").fill_null(0)
        
        for col in value_cols:
            col_reg = f"{col}_reg"
            recap_psap = recap_psap.with_columns(
                (pl.col(col) - pl.col(col_reg)).alias(col)
            ).drop(col_reg)
            
        # --- Étape 3 : Surcharges manuelles ---
        if manual_overrides:
            df_manual = pl.DataFrame(manual_overrides)
            
            # Conversion types si nécessaire
            for col in value_cols:
                if col in df_manual.columns:
                     df_manual = df_manual.with_columns(pl.col(col).cast(pl.Float64))

            # Filtrer et concaténer
            branches_manual = df_manual["Branche"].unique()
            recap_psap = recap_psap.filter(~pl.col("Branche").is_in(branches_manual))
            
            # On ne sélectionne que les années (pas de Total dans df_manual)
            # Si df_manual contient d'autres colonnes inutiles, on filtre
            cols_to_select = [c for c in ["Branche"] + value_cols if c in df_manual.columns]
            df_manual = df_manual.select(cols_to_select)
            
            # Ajouter colonnes manquantes dans df_manual (si 0)
            for col in value_cols:
                if col not in df_manual.columns:
                    df_manual = df_manual.with_columns(pl.lit(0.0).alias(col))
            
            # Ordonner colonnes
            df_manual = df_manual.select(["Branche"] + value_cols)
            
            recap_psap = pl.concat([recap_psap, df_manual], how="diagonal")
            
        # --- Étape 4 : Totaux ---
        # 1. Recalculer Colonne Total
        recap_psap = recap_psap.with_columns(
            pl.sum_horizontal(value_cols).alias("Total")
        )
        
        # 2. Trier et ajouter Ligne Total
        recap_psap = recap_psap.sort("Branche")
        
        cols_final = ["Branche"] + sorted(value_cols) + ["Total"]
        recap_psap = recap_psap.select(cols_final)
            
        totals = {"Branche": "TOTAL"}
        for col in cols_final[1:]: # Tout sauf Branche
            totals[col] = recap_psap[col].sum()
            
        recap_psap = pl.concat([recap_psap, pl.DataFrame([totals])])
        
        return recap_psap


    @staticmethod
    @st.cache_data(ttl=3600, show_spinner="Calcul IBNR Dommages Corporels...")
    def compute_ibnr_corporels(
        parquet_path: str,
        mapping: tuple,
        date_eval: date,
        loss_ratio_marche: float = 0.72
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Calcule l'IBNR pour la branche Dommages Corporels ligne par ligne.
        
        Méthodologie :
        1. Maturité M = T / D  (T = temps écoulé, D = durée totale)
        2. Facteur de retard LF = 0,95 - 0,90 × M  (plancher 5% si contrat expiré)
        3. Prime Acquise Pa = prime_nette × M
        4. Charge Théorique Ct = Pa × loss_ratio_marché (72%)
        5. IBNR = Ct × LF
        
        Retourne :
            - ibnr_by_exercice : détail par exercice (Exercice, IBNR, Prime_Acquise, Charge_Theorique, Nb_Contrats)
            - ibnr_pivot : tableau pivoté (Branche, 2022, 2023, ..., Total)
        """
        mapping_dict = dict(mapping)
        
        df = pl.read_parquet(parquet_path)
        
        # Préparer les données
        df = df.with_columns([
            pl.col("Exercice").fill_null("NR"),
            pl.col("PN_ACC").fill_null(0)
        ])
        
        # Appliquer le mapping
        df = df.with_columns(
            pl.col("Branche").cast(pl.Utf8).replace_strict(
                mapping_dict,
                default=pl.col("Branche")
            ).alias("Branche")
        )
        
        # Filtrer pour Dommages Corporels uniquement
        df = df.filter(pl.col("Branche") == "DOMMAGES CORPORELS")
        
        if df.height == 0:
            return pl.DataFrame(), pl.DataFrame()
        
        # ── Étape 1 : Maturité M ──
        # D = durée totale du contrat (jours)
        # T = max(0, min(D, date_évaluation - date_effet))
        df = df.with_columns([
            (pl.col("Echeance") - pl.col("Effet")).dt.total_days().cast(pl.Float64).alias("D"),
            (pl.lit(date_eval) - pl.col("Effet")).dt.total_days().cast(pl.Float64).alias("T_raw"),
        ])
        
        df = df.with_columns(
            pl.max_horizontal(
                pl.lit(0.0),
                pl.min_horizontal(pl.col("D"), pl.col("T_raw"))
            ).alias("T")
        )
        
        # M = T / D (si D > 0, sinon 0)
        df = df.with_columns(
            pl.when(pl.col("D") > 0)
              .then(pl.col("T") / pl.col("D"))
              .otherwise(0.0)
              .alias("M")
        )
        
        # ── Étape 2 : Facteur de Retard LF ──
        # LF = 0,95 - 0,90 × M
        # Si contrat expiré (date_eval >= echéance), LF = 0,05 (plancher de sécurité)
        df = df.with_columns(
            pl.when(pl.lit(date_eval) >= pl.col("Echeance"))
              .then(0.05)
              .otherwise(0.95 - 0.90 * pl.col("M"))
              .alias("LF")
        )
        
        # ── Étape 3 : Calcul IBNR ──
        # Pa = PN_ACC × M
        df = df.with_columns(
            (pl.col("PN_ACC") * pl.col("M")).alias("Prime_Acquise")
        )
        
        # Ct = Pa × loss_ratio_marché (0,72)
        df = df.with_columns(
            (pl.col("Prime_Acquise") * loss_ratio_marche).alias("Charge_Theorique")
        )
        
        # IBNR = Ct × LF
        df = df.with_columns(
            (pl.col("Charge_Theorique") * pl.col("LF")).alias("IBNR")
        )
        
        # ── Agrégation par Exercice ──
        ibnr_by_exercice = df.group_by("Exercice").agg([
            pl.sum("IBNR").alias("IBNR"),
            pl.sum("Prime_Acquise").alias("Prime_Acquise"),
            pl.sum("Charge_Theorique").alias("Charge_Theorique"),
            pl.len().alias("Nb_Contrats")
        ]).sort("Exercice")
        
        # ── Version pivotée (une ligne) ──
        ibnr_pivot_data = {"Branche": "DOMMAGES CORPORELS"}
        total_ibnr = 0.0
        for row in ibnr_by_exercice.to_dicts():
            ex = row["Exercice"]
            ibnr_val = row["IBNR"]
            ibnr_pivot_data[ex] = ibnr_val
            total_ibnr += ibnr_val
        ibnr_pivot_data["Total"] = total_ibnr
        
        ibnr_pivot = pl.DataFrame([ibnr_pivot_data])
        
        return ibnr_by_exercice, ibnr_pivot


def serialize_df(df: pl.DataFrame) -> bytes:
    """Sérialise un DataFrame pour le cache Streamlit."""
    buffer = io.BytesIO()
    df.serialize(buffer, format="binary")
    return buffer.getvalue()


def deserialize_df(data: bytes) -> pl.DataFrame:
    """Désérialise un DataFrame depuis le cache."""
    return pl.DataFrame.deserialize(io.BytesIO(data), format="binary")
