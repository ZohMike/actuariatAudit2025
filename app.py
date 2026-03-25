"""
Application principale - Dashboard Actuariat PREC/PA
Lancer avec: streamlit run app.py
"""
import streamlit as st
import polars as pl
from pathlib import Path

from config import APP_TITLE, APP_LAYOUT

# Services
from services.data_service import DataService
from services.mapping_service import MappingService
from services.calculation_service import CalculationService, serialize_df

# Composants
from components.sidebar import render_sidebar, display_data_stats
from components.mapping_form import render_mapping_form
from components.loss_ratio_form import render_loss_ratio_form, is_loss_ratios_validated
from components.results_display import ResultsDisplay


# Configuration
st.set_page_config(
    page_title=APP_TITLE,
    layout=APP_LAYOUT,
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour un meilleur style
st.markdown("""
<style>
    /* Style des en-têtes */
    h1, h2, h3 {
        color: #0e4c92;
    }
    
    /* Style des tableaux */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    /* Style des boutons */
    .stButton > button {
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    
    /* Style des métriques */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
        color: #0e4c92;
    }
    
    /* Style des expanders */
    .streamlit-expanderHeader {
        font-weight: 600;
        color: #0e4c92;
    }
    
    /* Style des dividers */
    hr {
        margin: 2rem 0;
        border-color: #e0e0e0;
    }
    
    /* Style des info/warning/success boxes */
    .stAlert {
        border-radius: 8px;
    }
    
    /* Style de la sidebar */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    
    /* Style des selectbox */
    .stSelectbox > div > div {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚀 Dashboard Actuariat - PREC/PA")
st.caption("Analyse de Production & Provisions")


# Sidebar
date_eval, frais_generaux = render_sidebar()


# Main: Chargement des données
st.header("📁 Chargement des Données de Production")
st.info("Chargez les fichiers pour chaque exercice ci-dessous.")

years = ["2022", "2023", "2024", "2025", "2026"]
files_by_year = {}
cols = st.columns(len(years))

for i, year in enumerate(years):
    with cols[i]:
        uploaded_files = st.file_uploader(
            f"Prod {year}",
            accept_multiple_files=True,
            type=['xlsx', 'xls', 'csv'],
            key=f"uploader_main_{year}"
        )
        if uploaded_files:
            files_by_year[year] = uploaded_files

if len(files_by_year) == len(years):
    st.success("✅ Tous les fichiers sont chargés")
    st.divider()

    with st.spinner("📦 Préparation des données..."):
        file_contents = DataService.prepare_file_contents(files_by_year)
        files_hash = DataService.compute_files_hash(file_contents)
        parquet_path = DataService.convert_excel_to_parquet(file_contents, files_hash)
        
        # Vérifier que le fichier existe
        if not Path(parquet_path).exists():
            st.cache_data.clear()
            parquet_path = DataService.convert_excel_to_parquet(file_contents, files_hash)
        
        DataService.cleanup_old_parquet_files(keep_hash=files_hash)
    
    stats = DataService.get_file_stats(parquet_path)
    display_data_stats(stats)
    
    st.success(f"✅ **{stats['rows']:,}** lignes chargées".replace(",", " "))
    
    # Diagnostic détaillé
    with st.expander("🔍 Diagnostic des données (cliquez pour vérifier les totaux)"):
        df_full = pl.read_parquet(parquet_path)
        
        st.write("**Aperçu (10 lignes):**")
        st.dataframe(df_full.head(10), use_container_width=True)
        
        st.divider()
        st.subheader("📊 Vérification des totaux AVANT mapping")
        
        # Remplacer les Exercice null par "NR" et les valeurs nulles par 0
        df_calc = df_full.with_columns([
            pl.col("Exercice").fill_null("NR"),
            pl.col("PN_ACC").fill_null(0)
        ])
        
        # Total général
        total_pe_general = df_calc.select(
            pl.col("PN_ACC").sum()
        ).item()
        st.metric("PE Total (PN_ACC, nulls=0)", f"{total_pe_general:,.2f}".replace(",", " "))
        
        # Par exercice
        st.write("**PE par Exercice (PN_ACC, nulls=0):**")
        pe_by_exercice = df_calc.group_by("Exercice").agg(
            pl.sum("PN_ACC").alias("PE"),
            pl.len().alias("Nb_Lignes")
        ).sort("Exercice")
        # Formater avec séparateurs
        pe_by_exercice_fmt = pe_by_exercice.with_columns([
            pl.col("PE").map_elements(lambda x: f"{x:,.2f}".replace(",", " "), return_dtype=pl.Utf8),
            pl.col("Nb_Lignes").map_elements(lambda x: f"{x:,}".replace(",", " "), return_dtype=pl.Utf8),
        ])
        st.dataframe(pe_by_exercice_fmt, use_container_width=True)
        
        # Par branche originale
        st.write("**PE par Branche originale (PN_ACC, nulls=0):**")
        pe_by_branche = df_calc.group_by("Branche").agg(
            pl.sum("PN_ACC").alias("PE"),
            pl.len().alias("Nb_Lignes")
        ).sort("Branche")
        # Formater avec séparateurs
        pe_by_branche_fmt = pe_by_branche.with_columns([
            pl.col("PE").map_elements(lambda x: f"{x:,.2f}".replace(",", " "), return_dtype=pl.Utf8),
            pl.col("Nb_Lignes").map_elements(lambda x: f"{x:,}".replace(",", " "), return_dtype=pl.Utf8),
        ])
        st.dataframe(pe_by_branche_fmt, use_container_width=True)

    # Mapping
    branches_observees = DataService.get_unique_branches(parquet_path)
    mapping = render_mapping_form(branches_observees)
    mapping_tuple = tuple(sorted(mapping.items()))
    
    # Vérification après mapping
    with st.expander("🔍 Vérification APRÈS mapping"):
        df_full = pl.read_parquet(parquet_path)
        mapping_dict = dict(mapping)
        
        # Remplacer les Exercice null par "NR" et les valeurs nulles par 0
        df_full = df_full.with_columns([
            pl.col("Exercice").fill_null("NR"),
            pl.col("PN_ACC").fill_null(0)
        ])
        
        # Appliquer le mapping
        df_mapped = df_full.with_columns(
            pl.col("Branche").cast(pl.Utf8).replace_strict(
                mapping_dict,
                default=pl.col("Branche")
            ).alias("Branche_Mappee")
        )
        
        st.write("**PE par Branche APRÈS mapping:**")
        pe_after_mapping = df_mapped.group_by(["Exercice", "Branche_Mappee"]).agg(
            pl.sum("PN_ACC").alias("PE"),
            pl.len().alias("Nb_Lignes")
        ).sort(["Exercice", "Branche_Mappee"])
        
        # Pivot pour affichage
        pe_pivot = pe_after_mapping.pivot(
            values="PE",
            index="Branche_Mappee",
            on="Exercice"
        ).fill_null(0)
        # Formater avec séparateurs
        formatted_cols = [pl.col("Branche_Mappee")]
        for col in pe_pivot.columns[1:]:
            formatted_cols.append(
                pl.col(col).map_elements(lambda x: f"{x:,.2f}".replace(",", " "), return_dtype=pl.Utf8).alias(col)
            )
        st.dataframe(pe_pivot.select(formatted_cols), use_container_width=True)
        
        # Total par exercice après mapping
        st.write("**Total PE par Exercice APRÈS mapping:**")
        total_by_ex = df_mapped.group_by("Exercice").agg(
            pl.sum("PN_ACC").alias("PE_Total")
        ).sort("Exercice")
        total_by_ex_fmt = total_by_ex.with_columns(
            pl.col("PE_Total").map_elements(lambda x: f"{x:,.2f}".replace(",", " "), return_dtype=pl.Utf8)
        )
        st.dataframe(total_by_ex_fmt, use_container_width=True)
    
    st.divider()
    
    # Loss Ratios
    branches_finales = sorted(set(mapping.values()))
    loss_ratios = render_loss_ratio_form(branches_finales)
    loss_ratios_tuple = tuple(sorted(loss_ratios.items()))
    
    # Calculs
    if MappingService.is_mapping_validated() and is_loss_ratios_validated():
        
        st.divider()
        st.success("✅ Configuration validée")
        
        # PE
        recap_pe = CalculationService.compute_recap_pe(parquet_path, mapping_tuple)
        recap_pe = CalculationService.compute_recap_pe(parquet_path, mapping_tuple)
        ResultsDisplay.display_production(recap_pe)
        st.text_area("💬 Commentaires (Production)", key="comment_recap_pe", height=200, value="""Etape 1 - Production Emise (PE)
Base reglementaire : Art. 334-8 du Code CIMA - Les provisions techniques sont assises sur les primes emises par branche d'assurance. Les comptes 7022 (primes emises) et 7023 (accessoires) du plan comptable CIMA (Art. 431) enregistrent ces flux.
La Prime Emise (PE) correspond a la prime nette augmentee des accessoires (PN + Acc) comptabilisee par exercice de souscription et par branche reglementaire.
Le tableau presente la ventilation de la PE par branche (apres mapping vers les branches cibles reglementaires conformes a la nomenclature CIMA) et par exercice comptable.
La colonne Total represente le cumul pluriannuel de la production par branche. La ligne TOTAL correspond a l'ensemble du portefeuille toutes branches confondues.
Ces montants constituent la base de calcul pour la determination de la PREC (Art. 334-8, 2 du Code CIMA) et des Primes Acquises (PA).""")

        
        st.divider()
        
        # Taux PREC
        last_exercice = DataService.get_last_exercice(parquet_path)
        df_taux = CalculationService.compute_taux_prec(
            parquet_path,
            mapping_tuple,
            loss_ratios_tuple,
            frais_generaux,
            last_exercice
        )
        ResultsDisplay.display_taux_prec(df_taux)
        st.text_area("💬 Commentaires (Taux PREC)", key="comment_taux_prec", height=240, value="""Etape 2 - Taux de Provision pour Risques En Cours (Taux PREC)
Base reglementaire : Art. 334-8, 2 du Code CIMA - La provision pour risques en cours est destinee a couvrir les risques et les frais generaux afferents, pour chacun des contrats a prime payable d'avance, a la periode comprise entre la date de l'inventaire et la prochaine echeance de prime.
Le taux PREC est determine branche par branche selon la formule :
  Taux PREC = Max(72 %, Loss Ratio + 0,5 x Frais Generaux repartis / PE)
- Le Loss Ratio (S/P) est le ratio sinistres-a-primes propre a chaque branche, saisi sur la base de l'experience historique du portefeuille et/ou des references marche.
- Les Frais Generaux sont repartis au prorata de la PE du dernier exercice par rapport a la PE totale du portefeuille, conformement a l'obligation de couverture des frais generaux afferents prevue par l'Art. 334-8, 2.
- Le plancher de 72 % est une norme prudentielle de la zone CIMA garantissant un provisionnement minimum adequate.
Ce taux sera applique a la quote-part non courue de chaque contrat pour constituer la PREC (comptes 3200/3201 du plan comptable CIMA).""")

        
        taux_prec_tuple = tuple(
            (row["Branche"], row["Taux_PREC"]) 
            for row in df_taux.select(["Branche", "Taux_PREC"]).to_dicts()
        )
        
        st.divider()
        
        # PREC
        recap_prec, erreurs = CalculationService.compute_prec(
            parquet_path,
            mapping_tuple,
            taux_prec_tuple,
            date_eval
        )
        ResultsDisplay.display_prec(recap_prec, erreurs)
        st.text_area("💬 Commentaires (PREC)", key="comment_recap_prec", height=260, value="""Etape 3 - Provision pour Risques En Cours (PREC)
Base reglementaire : Art. 334-8, 2 du Code CIMA - Comptes 3200 (primes emises par anticipation) et 3201 (autres primes) du plan comptable CIMA (Art. 431, Classe 3).
La PREC est calculee contrat par contrat (ligne a ligne) selon la methode du prorata temporis :
  PREC = PE x (Duree Restante / Duree Totale) x Taux PREC
- Duree Totale = Date d'Echeance - Date d'Effet (duree contractuelle en jours).
- Duree Restante = max(0, Date d'Echeance - Date d'Evaluation) : portion du risque non encore couru a la date d'inventaire.
- Taux PREC : taux determine a l'etape 2 pour la branche du contrat.
La PREC represente, conformement a l'Art. 334-8, 2, la part des primes emises correspondant a la couverture des risques et frais generaux afferents a la periode comprise entre la date de l'inventaire et la prochaine echeance. Elle vise a couvrir les sinistres futurs attendus sur la periode de garantie restante.
Les contrats dont la date d'effet est posterieure a la date d'echeance sont signales en anomalie.""")

        
        st.divider()
        
        # PA
        recap_pa = CalculationService.compute_pa(
            serialize_df(recap_pe),
            serialize_df(recap_prec)
        )
        ResultsDisplay.display_pa_1(recap_pa)
        st.text_area("💬 Commentaires (PA Globale)", key="comment_recap_pa", height=220, value="""Etape 4 - Primes Acquises Globales (PA)
Base reglementaire : Les Primes Acquises sont un indicateur derive de l'Art. 334-8, 2 du Code CIMA. Elles mesurent la fraction des primes emises couvrant la periode de risque effectivement ecoulee, par opposition a la PREC qui couvre la periode future.
Le compte 486 du plan comptable CIMA (Art. 431) enregistre les primes acquises et non emises nettes de commissions.
Formule :
  PA = PE - PREC
La PA represente la fraction des primes effectivement acquise a l'exercice, c'est-a-dire la part correspondant a la periode de risque deja ecoulee a la date d'evaluation.
Ce tableau constitue la base de revenus de souscription sur laquelle seront adossees les charges a l'ultime (CU) pour le calcul de la PSAP (Art. 334-8, 3).
Les PA sont presentees par branche reglementaire agregee (Automobile non encore eclatee a ce stade).""")

        
        st.divider()
        
        # PA 2 (Split)
        years = [col for col in recap_pa.columns if col not in ["Branche", "Total"]]
        dist_plat, dist_type = ResultsDisplay.display_split_params_form(years)
        
        recap_pa_split = CalculationService.compute_pa_split(
            serialize_df(recap_pa),
            dist_plat,
            dist_type
        )
        ResultsDisplay.display_pa_2(recap_pa_split)
        st.text_area("💬 Commentaires (PA Split)", key="comment_recap_pa_split", height=260, value="""Etape 4bis - Primes Acquises eclatees (PA Split)
Base reglementaire : La segmentation du risque automobile en RC (Responsabilite Civile obligatoire) et AR (Autres Risques / Dommages) repond aux exigences de suivi par categorie de l'Art. 334-8 du Code CIMA, qui impose un provisionnement adapte a la nature du risque couvert.
La branche Automobile est eclatee en deux sous-branches : AUTO RC et AUTO AR.
La repartition repose sur deux axes :
  1. Bassin de production : part Plateforme vs Hors Plateforme, variable selon l'exercice (refletant l'evolution du mode de distribution).
  2. Type de garantie : coefficients RC / AR appliques a chaque bassin (ex. Plateforme : 87 % RC / 13 % AR ; Hors : 82 % RC / 18 % AR).
Pour chaque exercice : PA_RC = PA_Auto x (Part_Plat x Coeff_RC_Plat + Part_Hors x Coeff_RC_Hors), et symetriquement pour AR.
Cette ventilation est necessaire pour appliquer des Loss Ratios differencie par sous-branche lors du calcul de la Charge Ultime, la RC automobile presentant structurellement une sinistralite plus lourde (sinistres corporels, cadence de reglement longue) que les dommages materiels.""")

        
        st.divider()
        
        # Loss Ratios 2 (Split)
        st.subheader("📊 Loss Ratios (PSAP) - Branches éclatées")
        st.info("Saisissez les Loss Ratios pour les branches éclatées (Auto RC / Auto AR).")
        
        # Extract branches from split PA
        branches_split = [
            str(b) for b in recap_pa_split["Branche"].to_list() 
            if str(b) != "TOTAL"
        ]
        branches_split.sort()
        
        loss_ratios_split = render_loss_ratio_form(branches_split, key_prefix="split")
        
        st.divider()
        
        # CU
        loss_ratios_split_tuple = tuple(sorted(loss_ratios_split.items()))
        recap_cu = ResultsDisplay.display_cu_form_and_results(recap_pa_split, loss_ratios_split_tuple)
        st.text_area("💬 Commentaires (CU)", key="comment_recap_cu", height=260, value="""Etape 5 - Charge a l'Ultime (CU)
Base reglementaire : La Charge a l'Ultime est une estimation actuarielle du cout final des sinistres, servant de base au calcul de la PSAP au sens de l'Art. 334-8, 3 du Code CIMA, qui definit la provision pour sinistres a payer comme la valeur estimative des depenses en principal et en frais, tant internes qu'externes, necessaires au reglement de tous les sinistres survenus et non payes.
Formule :
  CU = PA x Loss Ratio
- Le Loss Ratio (S/P) est defini par branche eclatee (AUTO RC, AUTO AR, etc.) sur la base de l'historique de sinistralite, des benchmarks sectoriels et du jugement d'expert.
- Les branches Sante et Dommages Corporels sont exclues du calcul CU standard : la Sante fait l'objet d'une saisie manuelle de PSAP, et les Dommages Corporels sont traites via la methode SAP + IBNR a l'etape 7.
La vue agregee regroupe AUTO RC et AUTO AR en AUTOMOBILE pour assurer la coherence avec la granularite des donnees sinistres (compte 3250 du plan comptable CIMA).
La CU sert de numerateur au calcul de la PSAP : PSAP = CU - Reglements cumules.""")

        
        st.divider()
        
        # Sinistres et SAP
        ResultsDisplay.display_sinistres_sap()
        st.text_area("💬 Commentaires (Sinistres)", key="comment_sinistres", height=280, value="""Etape 6 - Sinistres, Reglements et SAP
Base reglementaire : Art. 334-8, 3 du Code CIMA - La provision pour sinistres a payer est la valeur estimative des depenses en principal et en frais, tant internes qu'externes, necessaires au reglement de tous les sinistres survenus et non payes, y compris les capitaux constitutifs des rentes non encore mises a la charge de l'entreprise.
Comptes concernes (Art. 431) : 6020 (sinistres en principal), 6026 (frais accessoires), 6029 (recours en principal), 3250 (provision pour sinistres a payer).
Les donnees sinistres sont chargees par exercice de survenance et agregees par branche apres mapping vers les branches cibles.
Quatre indicateurs cles sont calcules :
  - Nombre de Sinistres : volumetrie de la sinistralite par branche et par exercice.
  - Cout Total : charge brute comptable (provisions dossier/dossier + reglements), representant l'estimation courante du cout final de chaque sinistre.
  - Reglements : montants effectivement decaisses au titre des sinistres (indemnites versees aux assures/beneficiaires).
  - SAP (Sinistres A Payer) = Cout Total - Reglements : encours residuel des sinistres connus non encore integralement regles.
La SAP reflete le stock de provisions dossier/dossier a la date d'evaluation. Elle sera combinee avec l'IBNR pour la branche Dommages Corporels, et les reglements seront soustraits de la CU pour les autres branches.""")

        
        st.divider()
        
        recap_psap = ResultsDisplay.display_psap_cu_minus_reg(
            recap_cu, recap_pe, parquet_path, mapping_tuple, date_eval,
            loss_ratios_split_tuple
        )
        st.text_area("💬 Commentaires (PSAP)", key="comment_recap_psap", height=340, value="""Etape 7 - Provision pour Sinistres A Payer (PSAP)
Base reglementaire : Art. 334-8, 3 du Code CIMA - Provision pour sinistres a payer : valeur estimative des depenses en principal et en frais, tant internes qu'externes, necessaires au reglement de tous les sinistres survenus et non payes, y compris les capitaux constitutifs des rentes non encore mises a la charge de l'entreprise.
Compte : 3250 du plan comptable CIMA (Art. 431, Classe 3).
La PSAP est constituee selon trois approches distinctes selon la branche :

1. Branches standard (Automobile, Incendie, RC, Transport, etc.) :
   PSAP = CU - Reglements cumules
   La Charge Ultime (CU) estime le cout final des sinistres ; les reglements deja verses en sont deduits pour obtenir la provision residuelle.

2. Dommages Corporels - Methode SAP + IBNR :
   PSAP = SAP + IBNR
   - SAP : provisions dossier/dossier issues des donnees sinistres (sinistres connus, non integralement regles).
   - IBNR (Incurred But Not Reported) : estimation des sinistres survenus mais non encore declares, calculee contrat par contrat :
     Maturite M = T/D (temps ecoule / duree totale du contrat)
     Facteur de retard LF = 0,95 - 0,90 x M (plancher 5 % si contrat expire)
     IBNR = Prime Acquise x Loss Ratio branche x LF
   L'Art. 334-8, 3 exige l'inclusion de tous les sinistres survenus et non payes, ce qui inclut explicitement les sinistres IBNR.

3. Sante : saisie manuelle de la PSAP sur la base d'analyses specifiques au portefeuille sante.

Le tableau PSAP Final consolide ces trois composantes pour obtenir la provision totale toutes branches confondues, en conformite avec les exigences de l'Art. 334-8 du Code CIMA.""")

        
        if recap_psap is not None:
            st.divider()
            
            # --- EXPORT GÉNÉRAL ---
            st.subheader("📥 Export Général")
            st.info("Téléchargez un rapport complet contenant tous les tableaux et commentaires.")
            
            from utils.export import create_full_report, create_full_report_pdf
            from services.calculation_service import deserialize_df
            
            # Rassembler toutes les données en liste ordonnée
            # (Titre, DataFrame/Commentaire)
            export_items = []
            
            # Production
            export_items.append(("--- PRODUCTION ---", st.session_state.get("comment_recap_pe", "")))
            export_items.append(("Production (PE)", recap_pe))
            
            # Taux PREC
            export_items.append(("--- TAUX PREC ---", st.session_state.get("comment_taux_prec", "")))
            export_items.append(("Taux PREC", df_taux))
            
            # PREC
            export_items.append(("--- PREC ---", st.session_state.get("comment_recap_prec", "")))
            export_items.append(("PREC", recap_prec))
            
            # PA
            export_items.append(("--- PA GLOBALE ---", st.session_state.get("comment_recap_pa", "")))
            export_items.append(("PA Globale", recap_pa))
            
            export_items.append(("--- PA SPLIT ---", st.session_state.get("comment_recap_pa_split", "")))
            export_items.append(("PA Split", recap_pa_split))
            
            # CU
            export_items.append(("--- CHARGE ULTIME ---", st.session_state.get("comment_recap_cu", "")))
            export_items.append(("CU Detail", recap_cu))
            export_items.append(("CU Auto Agrégé", CalculationService.compute_aggregated_cu(serialize_df(recap_cu))))
            
            # Sinistres
            sinistres_comment = st.session_state.get("comment_sinistres", "")
            if sinistres_comment or st.session_state.get('sinistres_recap_cout'):
                export_items.append(("--- SINISTRES ---", sinistres_comment))
                
                if st.session_state.get('sinistres_recap_nb'):
                    export_items.append(("Sinistres - Nombre", deserialize_df(st.session_state.get('sinistres_recap_nb'))))
                
                if st.session_state.get('sinistres_recap_cout'):
                    export_items.append(("Sinistres - Cout Total", deserialize_df(st.session_state.get('sinistres_recap_cout'))))
                
                if st.session_state.get('sinistres_recap_reg'):
                    export_items.append(("Sinistres - Règlements", deserialize_df(st.session_state.get('sinistres_recap_reg'))))
                    
                if st.session_state.get('sinistres_recap_sap'):
                    export_items.append(("Sinistres - SAP", deserialize_df(st.session_state.get('sinistres_recap_sap'))))
            
            # PSAP
            export_items.append(("--- PSAP ---", st.session_state.get("comment_recap_psap", "")))
            export_items.append(("PSAP Final", recap_psap))
            
            # Générer les rapports
            report_data_xlsx = create_full_report(export_items)
            report_data_pdf = create_full_report_pdf(export_items, date_eval=str(date_eval))
            
            col_xlsx, col_pdf = st.columns(2)
            with col_xlsx:
                st.download_button(
                    "📥 Télécharger le rapport (.xlsx)",
                    data=report_data_xlsx,
                    file_name=f"Rapport_Actuariat_{date_eval}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_full_report",
                    type="primary",
                    use_container_width=True
                )
            with col_pdf:
                st.download_button(
                    "📥 Télécharger le rapport (.pdf)",
                    data=report_data_pdf,
                    file_name=f"Rapport_Actuariat_{date_eval}.pdf",
                    mime="application/pdf",
                    key="dl_full_report_pdf",
                    type="primary",
                    use_container_width=True
                )
        
    else:
        st.warning("⚠️ Validez le mapping et les Loss Ratios pour lancer les calculs.")
        
        col1, col2 = st.columns(2)
        with col1:
            if MappingService.is_mapping_validated():
                st.success("✅ Mapping validé")
            else:
                st.info("⏳ Mapping en attente")
        with col2:
            if is_loss_ratios_validated():
                st.success("✅ Loss Ratios validés")
            else:
                st.info("⏳ Loss Ratios en attente")

else:
    if files_by_year:
        missing = set(years) - set(files_by_year.keys())
        st.warning(f"⏳ En attente des fichiers pour : {', '.join(sorted(missing))}")
        st.info("Veuillez charger tous les fichiers pour lancer le traitement.")
    else:
        st.info("📁 Chargez vos fichiers de production ci-dessus.")
    
    with st.expander("ℹ️ Guide"):
        st.markdown("""
        ### Étapes:
        1. Téléchargez le template
        2. Chargez vos fichiers Excel/CSV
        3. Mappez les branches
        4. Saisissez les Loss Ratios
        5. Consultez les résultats
        
        ### Colonnes requises:
        `Exercice`, `Police`, `Client`, `Branche`, `Categorie`, 
        `Intermediaire`, `PN_ACC`, `Effet`, `Echeance`
        """)
