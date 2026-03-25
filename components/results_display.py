"""
Composant d'affichage des résultats
"""
import streamlit as st
import polars as pl
import pandas as pd
from services.calculation_service import CalculationService, serialize_df, deserialize_df
from services.data_service import DataService
from services.sinistres_service import SinistresService


def format_number(x):
    """Formate un nombre avec séparateurs de milliers (espace) et 2 décimales."""
    if pd.isna(x) or x is None:
        return ""
    try:
        return f"{float(x):,.2f}".replace(",", " ")
    except:
        return str(x)



def style_dataframe(df: pl.DataFrame, highlight_total: bool = False) -> None:
    """
    Affiche un DataFrame avec un style amélioré.
    En-tête en bleu foncé uniquement.
    """
    # Convertir en pandas pour le styling
    pdf = df.to_pandas()
    
    # Identifier les colonnes numériques
    numeric_cols = [col for col in pdf.columns if col != "Branche" and pdf[col].dtype in ['float64', 'int64', 'float32', 'int32']]
    
    # Formater les nombres
    for col in numeric_cols:
        pdf[col] = pdf[col].apply(format_number)
    
    # Style général
    styled = pdf.style.set_properties(**{
        'text-align': 'right',
        'padding': '8px 12px',
        'border': '1px solid #ddd'
    }).set_properties(
        subset=['Branche'] if 'Branche' in pdf.columns else [],
        **{'text-align': 'left', 'font-weight': '500'}
    ).set_table_styles([
        {'selector': 'th', 'props': [
            ('background-color', '#0e4c92'),
            ('color', 'white'),
            ('font-weight', 'bold'),
            ('text-align', 'center'),
            ('padding', '10px 12px'),
            ('border', '1px solid #0a3d73')
        ]},
        {'selector': 'tbody tr:hover', 'props': [
            ('background-color', '#f5f5f5')
        ]},
        {'selector': 'table', 'props': [
            ('border-collapse', 'collapse'),
            ('width', '100%'),
            ('margin', '10px 0')
        ]}
    ])
    
    st.dataframe(styled, use_container_width=True, hide_index=True)


def format_dataframe_numbers(df: pl.DataFrame) -> pl.DataFrame:
    """Formate les colonnes numériques avec séparateurs de milliers."""
    formatted_cols = []
    for col in df.columns:
        if df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
            # Formater avec séparateurs de milliers
            formatted_cols.append(
                pl.col(col).map_elements(
                    lambda x: f"{x:,.2f}".replace(",", " ") if x is not None else "",
                    return_dtype=pl.Utf8
                ).alias(col)
            )
        else:
            formatted_cols.append(pl.col(col))
    return df.select(formatted_cols)


class ResultsDisplay:
    """Classe pour l'affichage des résultats des calculs"""
    
    @staticmethod
    def display_production(recap_pe: pl.DataFrame) -> None:
        """Affiche le tableau de production (PE)"""
        st.header("📈 Étape 1: Production (PE = PN + Acc)")
        
        style_dataframe(recap_pe)
    
    @staticmethod
    def display_taux_prec(df_taux: pl.DataFrame) -> None:
        """Affiche le tableau des taux PREC"""
        st.header("📐 Étape 2: Taux PREC")
        st.info("Formule: Max(72%, LR + 0.5 × FG / PE)")
        
        # Préparer l'affichage avec formatage spécial pour les pourcentages
        pdf = df_taux.to_pandas()
        pdf["Loss_Ratio"] = pdf["Loss_Ratio"].apply(lambda x: f"{x:.2%}" if pd.notna(x) else "")
        pdf["Taux_PREC"] = pdf["Taux_PREC"].apply(lambda x: f"{x:.2%}" if pd.notna(x) else "")
        pdf["FG_Reparti"] = pdf["FG_Reparti"].apply(format_number)
        
        styled = pdf.style.set_properties(**{
            'text-align': 'center',
            'padding': '8px 12px',
            'border': '1px solid #ddd'
        }).set_properties(
            subset=['Branche'],
            **{'text-align': 'left', 'font-weight': '500'}
        ).set_table_styles([
            {'selector': 'th', 'props': [
                ('background-color', '#0e4c92'),
                ('color', 'white'),
                ('font-weight', 'bold'),
                ('text-align', 'center'),
                ('padding', '10px 12px')
            ]}
        ])
        
        st.dataframe(styled, use_container_width=True, hide_index=True)
    
    @staticmethod
    def display_prec(recap_prec: pl.DataFrame, erreurs: pl.DataFrame) -> None:
        """Affiche le tableau PREC"""
        st.header("🔄 Étape 3: PREC")
        
        if not erreurs.is_empty():
            st.warning(f"⚠️ {len(erreurs)} ligne(s) avec date effet > échéance")
            with st.expander("Voir les erreurs"):
                st.dataframe(erreurs, use_container_width=True, hide_index=True)
        
        style_dataframe(recap_prec)
    
    @staticmethod
    def display_pa_1(recap_pa: pl.DataFrame) -> None:
        """Affiche le tableau des Primes Acquises 1"""
        st.header("💰 Étape 4: Primes Acquises 1 (PA1)")
        
        style_dataframe(recap_pa)

    @staticmethod
    def display_split_params_form(years: list) -> tuple:
        """Affiche le formulaire pour la répartition Automobile."""
        st.header("🔀 Paramètres de répartition Automobile")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Bassin de production")
            st.info("Répartition Plateforme vs Hors Plateforme")
            
            # Default values from user request
            default_plat = [
                {"Annee": "2022", "Plateforme": 0.0, "Hors": 1.0},
                {"Annee": "2023", "Plateforme": 0.65, "Hors": 0.35},
                {"Annee": "2024", "Plateforme": 0.78, "Hors": 0.22},
                {"Annee": "2025", "Plateforme": 0.67, "Hors": 0.33},
            ]
            
            # Ensure we cover the actual years in the data if different
            # For now, we use the hardcoded defaults + any extra years from data as 0/100 default
            existing_years = {d["Annee"] for d in default_plat}
            for y in years:
                if y not in existing_years:
                     default_plat.append({"Annee": y, "Plateforme": 0.0, "Hors": 1.0})
            
            # Sort by year
            default_plat.sort(key=lambda x: x["Annee"])
            
            df_plat = pd.DataFrame(default_plat)
            
            edited_plat = st.data_editor(
                df_plat,
                column_config={
                    "Annee": st.column_config.TextColumn("Année", disabled=True),
                    "Plateforme": st.column_config.NumberColumn(
                        "Plateforme (%)", 
                        min_value=0, 
                        max_value=1, 
                        format="%.2f"
                    ),
                    "Hors": st.column_config.NumberColumn(
                        "Hors (%)", 
                        min_value=0, 
                        max_value=1, 
                        format="%.2f"
                    ),
                },
                hide_index=True,
                key="editor_plat"
            )
            
        with col2:
            st.subheader("Type de Garantie")
            st.info("Répartition RC vs AR par bassin")
            
            # Default values
            default_type = [
                {"Type": "AUTO RC", "Plateforme": 0.87, "Hors": 0.82},
                {"Type": "AUTO AR", "Plateforme": 0.13, "Hors": 0.18},
            ]
            
            df_type = pd.DataFrame(default_type)
            
            edited_type = st.data_editor(
                df_type,
                column_config={
                    "Type": st.column_config.TextColumn("Type", disabled=True),
                    "Plateforme": st.column_config.NumberColumn(
                        "Plateforme (%)", 
                        min_value=0, 
                        max_value=1, 
                        format="%.2f"
                    ),
                    "Hors": st.column_config.NumberColumn(
                        "Hors (%)", 
                        min_value=0, 
                        max_value=1, 
                        format="%.2f"
                    ),
                },
                hide_index=True,
                key="editor_type"
            )

        return edited_plat.to_dict('records'), edited_type.to_dict('records')

    @staticmethod
    def display_pa_2(recap_pa_split: pl.DataFrame) -> None:
        """Affiche le tableau des Primes Acquises 2 (Split)"""
        st.header("💰 Étape 4bis: Primes Acquises 2 (PA2 - Split)")
        style_dataframe(recap_pa_split)

    
    @staticmethod
    def display_cu_form_and_results(
        recap_pa: pl.DataFrame,
        loss_ratios_tuple: tuple
    ) -> pl.DataFrame:
        """Affiche le formulaire CU et les résultats (Santé/Corporels masqués)."""
        st.header("📊 Étape 5: Charge à l'Ultime (CU)")
        
        # Plus de saisie manuelle ici pour Santé/Corporels
        st.info("Les branches 'Santé' et 'Dommages Corporels' sont gérées directement à l'étape PSAP.")
        
        # Calcul standard (les branches manuelles seront filtrées pour l'affichage)
        recap_cu = CalculationService.compute_cu(
            serialize_df(recap_pa),
            loss_ratios_tuple
        )
        
        # Filtrer pour l'affichage uniquement
        branches_masquees = ["SANTE", "DOMMAGES CORPORELS"]
        recap_cu_display = recap_cu.filter(~pl.col("Branche").is_in(branches_masquees))
        
        style_dataframe(recap_cu_display)
        
        # Affichage version agrégée (Automobile regroupée)
        st.subheader("Vue agrégée (Automobile regroupée)")
        recap_cu_agg = CalculationService.compute_aggregated_cu(serialize_df(recap_cu))
        # Appliquer le même filtre masqué si nécessaire (Santé/Corp sont déjà masqués par filtrage ou non?)
        # Non, on doit refiltrer si on veut les masquer aussi ici
        recap_cu_agg_display = recap_cu_agg.filter(~pl.col("Branche").is_in(branches_masquees))
        style_dataframe(recap_cu_agg_display)
        
        return recap_cu
    
    @staticmethod
    def display_sinistres_sap() -> None:
        """Affiche la section Sinistres et SAP avec mapping dédié"""
        from services.mapping_service import MappingService
        from config import BRANCHES_CIBLES
        
        st.header("🏦 Étape 6: Sinistres, Règlements et SAP")
        
        from utils.templates import create_sinistres_template
        
        col_info, col_download = st.columns([3, 1])
        with col_info:
            st.info("""
            📁 Chargez les fichiers sinistres pour chaque année.  
            Colonnes requises: **BRANCHE**, **REGLEMENT**, **COUT_TOTAL**
            """)
        with col_download:
            st.download_button(
                "📥 Télécharger template",
                data=create_sinistres_template(),
                file_name="template_sinistres.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        # Upload des fichiers par année
        years = ["2022", "2023", "2024", "2025", "2026"]
        uploaded_files = {}
        
        cols = st.columns(len(years))
        for i, year in enumerate(years):
            with cols[i]:
                file = st.file_uploader(
                    f"📁 Sinistres {year}",
                    type=['xlsx', 'xls', 'csv'],
                    key=f"sin_file_{year}"
                )
                if file:
                    uploaded_files[year] = file
        
        if not uploaded_files:
            st.warning("⏳ Chargez au moins un fichier sinistres pour continuer")
            return
        
        # Préparer les données
        file_contents = []
        for year, file in uploaded_files.items():
            content = file.read()
            file.seek(0)
            file_contents.append((file.name, content, year))
        
        try:
            # Étape 1: Extraire les branches uniques
            branches_sinistres = SinistresService.get_unique_branches(file_contents)
            
            st.success(f"✅ {len(uploaded_files)} fichier(s) chargé(s)")
            
            st.divider()
            
            # Afficher les branches trouvées
            st.subheader(f"📋 Branches trouvées dans les fichiers sinistres ({len(branches_sinistres)})")
            
            # Afficher en colonnes pour une meilleure lisibilité
            n_display_cols = 4
            display_cols = st.columns(n_display_cols)
            for i, branche in enumerate(branches_sinistres):
                with display_cols[i % n_display_cols]:
                    st.write(f"• {branche}")
            
            st.divider()
            
            # Étape 2: Mapping des branches sinistres
            st.subheader("🔀 Mapping des branches sinistres → Branches cibles")
            
            with st.form("sinistres_mapping_form"):
                st.write("Pour chaque branche trouvée, sélectionnez la branche cible correspondante :")
                st.write("")
                
                mapping_sinistres = {}
                
                # Afficher en colonnes
                n_cols = 3
                cols = st.columns(n_cols)
                
                for i, branche in enumerate(branches_sinistres):
                    col_idx = i % n_cols
                    with cols[col_idx]:
                        # Trouver l'index par défaut
                        default_idx = MappingService.find_default_mapping_index(branche, BRANCHES_CIBLES)
                        
                        mapping_sinistres[branche] = st.selectbox(
                            f"**{branche}**",
                            BRANCHES_CIBLES,
                            index=default_idx,
                            key=f"sin_map_{branche}"
                        )
                
                st.write("")
                mapping_validated = st.form_submit_button(
                    "✅ Valider le mapping et calculer",
                    type="primary",
                    use_container_width=True
                )
            
            if mapping_validated:
                st.session_state['sinistres_mapping'] = mapping_sinistres
                st.session_state['sinistres_mapping_validated'] = True
            
            # Étape 3: Calcul et affichage si mapping validé
            if st.session_state.get('sinistres_mapping_validated', False):
                mapping_dict = st.session_state.get('sinistres_mapping', {})
                mapping_tuple = tuple(sorted(mapping_dict.items()))
                
                st.divider()
                
                # Charger et traiter les sinistres
                df_sinistres = SinistresService.load_sinistres_files(
                    file_contents,
                    mapping_tuple
                )
                
                # Afficher les données détaillées
                with st.expander("📋 Données agrégées par Branche/Exercice"):
                    style_dataframe(df_sinistres, highlight_total=False)
                
                st.divider()
                
                # Tableau Nombre de Sinistres
                st.subheader("🔢 Nombre de Sinistres par Branche")
                recap_nb = SinistresService.pivot_sinistres(df_sinistres, "Nombre_Sinistres")
                
                # Formatage spécifique pour entiers (pas de décimales)
                recap_nb_fmt = recap_nb.select([
                    pl.col("Branche"),
                    pl.exclude("Branche").map_elements(
                        lambda x: f"{int(x):,}".replace(",", " ") if x is not None else "0",
                        return_dtype=pl.Utf8
                    )
                ])
                style_dataframe(recap_nb_fmt)
                st.session_state['sinistres_recap_nb'] = serialize_df(recap_nb)
                
                st.divider()
                
                # Tableau Coût Total
                st.subheader("💰 Coût Total par Branche")
                recap_cout = SinistresService.pivot_sinistres(df_sinistres, "Cout_Total")
                style_dataframe(recap_cout)
                st.session_state['sinistres_recap_cout'] = serialize_df(recap_cout)
                
                st.divider()
                
                # Tableau Règlements
                st.subheader("💵 Total Règlements par Branche")
                recap_reg = SinistresService.pivot_sinistres(df_sinistres, "Total_Reglement")
                style_dataframe(recap_reg)
                
                # Stocker pour l'étape 7 (PSAP = CU - Règlement)
                st.session_state['sinistres_recap_reg'] = serialize_df(recap_reg)
                
                st.divider()
                
                # Tableau SAP (Coût Total - Règlements)
                st.subheader("📊 SAP = Coût Total - Règlements")
                recap_sap = SinistresService.pivot_sinistres(df_sinistres, "SAP")
                style_dataframe(recap_sap)
                st.session_state['sinistres_recap_sap'] = serialize_df(recap_sap)

            else:
                st.info("⏳ Validez le mapping pour voir les résultats")
                
        except ValueError as e:
            st.error(f"❌ Erreur: {e}")
            st.info("Vérifiez que vos fichiers contiennent les colonnes: **BRANCHE**, **REGLEMENT**, **COUT_TOTAL**")
        except Exception as e:
            st.error(f"❌ Erreur inattendue: {e}")
    
    @staticmethod
    def display_psap_cu_minus_reg(
        recap_cu: pl.DataFrame,
        recap_pe: pl.DataFrame,
        parquet_path: str = None,
        mapping_tuple: tuple = None,
        date_eval=None,
        loss_ratios_split: tuple = None
    ) -> pl.DataFrame:
        """
        Affiche l'étape 7: PSAP = CU - Règlement.
        - Dommages Corporels : PSAP = SAP + IBNR (automatique)
        - Santé : saisie manuelle
        - Autres branches : PSAP = CU - Règlement
        """
        st.header("📐 Étape 7: Calcul de la PSAP")
        st.info("**PSAP = SAP + IBNR** pour Dommages Corporels | **Saisie manuelle** pour Santé | **CU − Règlement** pour les autres branches")
        
        recap_reg_data = st.session_state.get('sinistres_recap_reg')
        if not recap_reg_data:
            st.warning("⏳ Chargez et validez les sinistres (étape 6) pour afficher la PSAP.")
            return None
            
        recap_reg = deserialize_df(recap_reg_data)
        
        exercices = [col for col in recap_cu.columns if col not in ["Branche", "Total"]]
        
        # ═══════════════════════════════════════════════════════════════
        # SECTION IBNR — Dommages Corporels
        # ═══════════════════════════════════════════════════════════════
        st.subheader("🧮 IBNR — Dommages Corporels")
        
        # Extraire le Loss Ratio de la branche Dommages Corporels depuis les LR split
        lr_corporels = 0.72  # Valeur par défaut
        if loss_ratios_split:
            lr_dict = dict(loss_ratios_split)
            lr_corporels = lr_dict.get("DOMMAGES CORPORELS", 0.72)
        
        lr_pct = f"{lr_corporels:.0%}"
        
        st.markdown(rf"""
**Méthodologie** : calcul ligne par ligne sur chaque contrat de la branche.

| Étape | Formule |
|-------|---------|
| Maturité | $M = T / D$ &nbsp; où $T = \max(0,\,\min(D,\,\text{{date\_eval}} - \text{{date\_effet}}))$ |
| Facteur de retard | $LF = 0{{,}}95 - 0{{,}}90 \times M$ &nbsp; (plancher 5 % si contrat expiré) |
| Prime acquise | $P_a = \text{{prime\_nette}} \times M$ |
| Charge théorique | $C_t = P_a \times {lr_pct}$ &nbsp; (Loss Ratio branche) |
| **IBNR** | $C_t \times LF$ |
        """)
        
        st.success(f"**Loss Ratio utilisé** : {lr_corporels:.2%} (issu de la saisie Loss Ratios split)")
        
        calculated_psap_corp = {}
        
        if parquet_path and mapping_tuple and date_eval:
            ibnr_by_exercice, ibnr_pivot = CalculationService.compute_ibnr_corporels(
                parquet_path, mapping_tuple, date_eval, lr_corporels
            )
            
            if not ibnr_pivot.is_empty():
                # Détail IBNR par exercice
                with st.expander("📋 Détail IBNR par exercice (Prime Acquise, Charge Théorique, IBNR)"):
                    style_dataframe(format_dataframe_numbers(ibnr_by_exercice))
                
                st.write("**IBNR par exercice :**")
                style_dataframe(format_dataframe_numbers(ibnr_pivot))
                
                # Récupérer SAP Dommages Corporels depuis les sinistres
                recap_sap_data = st.session_state.get('sinistres_recap_sap')
                
                if recap_sap_data:
                    recap_sap = deserialize_df(recap_sap_data)
                    sap_corp = recap_sap.filter(pl.col("Branche") == "DOMMAGES CORPORELS")
                    
                    if not sap_corp.is_empty():
                        st.write("**SAP Dommages Corporels (issue des sinistres) :**")
                        style_dataframe(format_dataframe_numbers(sap_corp))
                        
                        # ── PSAP = SAP + IBNR ──
                        st.subheader("📊 PSAP Dommages Corporels = SAP + IBNR")
                        
                        psap_corp_data = {"Branche": "DOMMAGES CORPORELS"}
                        detail_rows = []
                        total_sap = 0.0
                        total_ibnr = 0.0
                        total_psap = 0.0
                        
                        for ex in exercices:
                            sap_val = 0.0
                            ibnr_val = 0.0
                            
                            if ex in sap_corp.columns:
                                try:
                                    sap_val = float(sap_corp.select(pl.col(ex)).item())
                                except Exception:
                                    pass
                            
                            if ex in ibnr_pivot.columns:
                                try:
                                    ibnr_val = float(ibnr_pivot.select(pl.col(ex)).item())
                                except Exception:
                                    pass
                            
                            psap_val = sap_val + ibnr_val
                            psap_corp_data[ex] = psap_val
                            calculated_psap_corp[ex] = psap_val
                            
                            detail_rows.append({
                                "Exercice": ex,
                                "SAP": sap_val,
                                "IBNR": ibnr_val,
                                "PSAP (SAP+IBNR)": psap_val
                            })
                            total_sap += sap_val
                            total_ibnr += ibnr_val
                            total_psap += psap_val
                        
                        # Tableau détaillé SAP + IBNR = PSAP
                        detail_rows.append({
                            "Exercice": "TOTAL",
                            "SAP": total_sap,
                            "IBNR": total_ibnr,
                            "PSAP (SAP+IBNR)": total_psap
                        })
                        df_detail = pl.DataFrame(detail_rows)
                        style_dataframe(format_dataframe_numbers(df_detail))
                        
                        # Métriques résumé
                        col_m1, col_m2, col_m3 = st.columns(3)
                        col_m1.metric("SAP Total", f"{total_sap:,.0f}".replace(",", " "))
                        col_m2.metric("IBNR Total", f"{total_ibnr:,.0f}".replace(",", " "))
                        col_m3.metric("PSAP Total (SAP + IBNR)", f"{total_psap:,.0f}".replace(",", " "))
                        
                        st.success("✅ PSAP Dommages Corporels calculée automatiquement (SAP + IBNR)")
                    else:
                        st.warning("Pas de données SAP pour Dommages Corporels dans les sinistres.")
                else:
                    st.warning("Veuillez charger les sinistres pour obtenir la SAP.")
            else:
                st.warning("Pas de contrats Dommages Corporels dans les données de production.")
        else:
            st.warning("Paramètres manquants pour le calcul IBNR (chemin données, mapping ou date d'évaluation).")
        
        st.divider()
        
        # ═══════════════════════════════════════════════════════════════
        # SAISIE MANUELLE — Santé uniquement
        # ═══════════════════════════════════════════════════════════════
        st.subheader("✍️ Saisie manuelle PSAP (Santé)")
        st.info("Saisissez les montants PSAP directement pour la branche Santé.")
        
        from utils.templates import create_psap_template
        
        col_dl, col_ul = st.columns([1, 2])
        
        with col_dl:
            st.download_button(
                "📥 Télécharger template PSAP",
                data=create_psap_template(exercices),
                file_name="template_psap_manuel.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_psap_template"
            )
            
        uploaded_psap = None
        with col_ul:
            uploaded_psap = st.file_uploader(
                "📂 Charger fichier PSAP rempli (Optionnel)",
                type=['xlsx', 'xls'],
                key="ul_psap_manual",
                label_visibility="collapsed"
            )
        
        # Données par défaut : Santé uniquement
        default_data = [
            {"Branche": "SANTE", **{ex: 0.0 for ex in exercices}},
        ]
        
        if uploaded_psap:
            try:
                df_uploaded = pd.read_excel(uploaded_psap)
                required_cols = {"Branche"}
                if not required_cols.issubset(df_uploaded.columns):
                    st.error("Colonnes manquantes dans le fichier. Utilisez le template.")
                else:
                    df_uploaded = df_uploaded[df_uploaded["Branche"].isin(["SANTE"])]
                    new_data = []
                    for _, row in df_uploaded.iterrows():
                        item = {"Branche": row["Branche"]}
                        for ex in exercices:
                            val = 0.0
                            if ex in df_uploaded.columns:
                                val = float(row[ex])
                            elif int(ex) in df_uploaded.columns:
                                val = float(row[int(ex)])
                            item[ex] = val
                        new_data.append(item)
                    if new_data:
                        default_data = new_data
                        st.success("✅ Fichier chargé avec succès")
            except Exception as e:
                st.error(f"Erreur lecture fichier: {e}")
        
        df_manual = pd.DataFrame(default_data)
        
        column_config = {
            "Branche": st.column_config.TextColumn("Branche", disabled=True),
        }
        for ex in exercices:
            column_config[ex] = st.column_config.NumberColumn(
                f"{ex}",
                min_value=0.0,
                format="%.2f",
                required=True
            )
            
        edited_df = st.data_editor(
            df_manual,
            column_config=column_config,
            hide_index=True,
            key=f"psap_manual_editor_{uploaded_psap.name if uploaded_psap else 'default'}",
            use_container_width=True
        )
        
        # Préparer les surcharges manuelles : Santé (formulaire) + Dommages Corporels (SAP+IBNR)
        manual_overrides = edited_df.to_dict('records')
        
        # Ajouter la surcharge Dommages Corporels calculée automatiquement
        if calculated_psap_corp:
            corp_override = {"Branche": "DOMMAGES CORPORELS"}
            for ex in exercices:
                corp_override[ex] = calculated_psap_corp.get(ex, 0.0)
            manual_overrides.append(corp_override)
        
        st.divider()
        
        # ═══════════════════════════════════════════════════════════════
        # TABLEAU PSAP FINAL
        # ═══════════════════════════════════════════════════════════════
        st.subheader("📊 Tableau PSAP Final")
        st.info("**Dommages Corporels** : SAP + IBNR | **Santé** : saisie manuelle | **Autres** : CU − Règlement")
        
        recap_psap = CalculationService.compute_psap(
            serialize_df(recap_cu),
            serialize_df(recap_reg),
            manual_overrides
        )
        
        style_dataframe(recap_psap)
        
        return recap_psap
