"""
Composant Formulaire de saisie des Loss Ratios
- Option 1: Upload fichier Excel
- Option 2: Saisie manuelle
"""
import streamlit as st
import polars as pl
import pandas as pd
import io
from config import DEFAULT_LOSS_RATIO


def render_loss_ratio_form(branches: list[str], key_prefix: str = "") -> dict:
    """Affiche le formulaire de saisie des Loss Ratios."""
    st.header(f"📊 Saisie des Loss Ratios {key_prefix}")
    
    # Choix du mode de saisie
    mode = st.radio(
        "Mode de saisie",
        ["📁 Upload fichier Excel", "✏️ Saisie manuelle"],
        horizontal=True,
        key=f"lr_mode_{key_prefix}"
    )
    
    if mode == "📁 Upload fichier Excel":
        return _render_upload_mode(branches, key_prefix)
    else:
        return _render_manual_mode(branches, key_prefix)


def _render_upload_mode(branches: list[str], key_prefix: str) -> dict:
    """Mode upload fichier Excel."""
    
    # Template téléchargeable
    st.info("Le fichier doit contenir les colonnes: **Branche** et **Loss_Ratio**")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Créer template
        template_data = {
            "Branche": branches,
            "Loss_Ratio": [DEFAULT_LOSS_RATIO] * len(branches)
        }
        template_df = pd.DataFrame(template_data)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            template_df.to_excel(writer, index=False, sheet_name='Loss_Ratios')
        
        st.download_button(
            label="📥 Télécharger template Loss Ratios",
            data=output.getvalue(),
            file_name=f"template_loss_ratios_{key_prefix}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
            key=f"dl_template_{key_prefix}"
        )
    
    with col2:
        uploaded_file = st.file_uploader(
            "📁 Charger fichier Loss Ratios",
            type=['xlsx', 'xls', 'csv'],
            key=f"lr_file_upload_{key_prefix}"
        )
    
    if uploaded_file:
        try:
            # Lire le fichier
            if uploaded_file.name.endswith('.csv'):
                df_lr = pl.read_csv(uploaded_file)
            else:
                df_lr = pl.read_excel(uploaded_file)
            
            # Vérifier les colonnes
            if "Branche" not in df_lr.columns or "Loss_Ratio" not in df_lr.columns:
                st.error("❌ Le fichier doit contenir les colonnes 'Branche' et 'Loss_Ratio'")
                return _get_default_loss_ratios(branches)
            
            # Afficher aperçu
            st.success(f"✅ Fichier chargé: {len(df_lr)} branches")
            st.dataframe(df_lr, width="stretch", hide_index=True)
            
            # Convertir en dictionnaire
            loss_ratios = {}
            for row in df_lr.to_dicts():
                branche = str(row["Branche"]).strip()
                lr = float(row["Loss_Ratio"])
                loss_ratios[branche] = lr
            
            # Ajouter les branches manquantes avec valeur par défaut
            for branche in branches:
                if branche not in loss_ratios:
                    loss_ratios[branche] = DEFAULT_LOSS_RATIO
                    st.warning(f"⚠️ Branche '{branche}' non trouvée dans le fichier, valeur par défaut: {DEFAULT_LOSS_RATIO}")
            
            # Bouton de validation
            if st.button("✅ Valider les Loss Ratios", type="primary", width="stretch", key=f"validate_lr_upload_{key_prefix}"):
                for branche, lr in loss_ratios.items():
                    st.session_state[f'lr_saved_{branche}_{key_prefix}'] = lr
                st.session_state[f'loss_ratios_valides_{key_prefix}'] = loss_ratios
                st.success("✅ Loss Ratios validés!")
                st.rerun()
            
            return st.session_state.get(f'loss_ratios_valides_{key_prefix}', loss_ratios)
            
        except Exception as e:
            st.error(f"❌ Erreur lors de la lecture du fichier: {e}")
            return _get_default_loss_ratios(branches)
    
    else:
        st.warning("⏳ Veuillez charger un fichier ou passer en mode saisie manuelle")
        return st.session_state.get(f'loss_ratios_valides_{key_prefix}', _get_default_loss_ratios(branches))


def _render_manual_mode(branches: list[str], key_prefix: str) -> dict:
    """Mode saisie manuelle."""
    
    with st.form(f"loss_ratios_form_{key_prefix}"):
        st.info("Entrez le Loss Ratio (entre 0 et 1) pour chaque branche.")
        
        loss_ratios = {}
        nb_cols = min(len(branches), 4)
        cols = st.columns(nb_cols)
        
        for i, branche in enumerate(branches):
            with cols[i % nb_cols]:
                saved_value = st.session_state.get(f'lr_saved_{branche}_{key_prefix}', DEFAULT_LOSS_RATIO)
                
                loss_ratios[branche] = st.number_input(
                    f"{branche}",
                    min_value=0.0,
                    max_value=1.0,
                    value=saved_value,
                    step=0.01,
                    format="%.2f",
                    key=f"lr_input_{branche}_{key_prefix}"
                )
        
        submitted = st.form_submit_button(
            "✅ Valider les Loss Ratios",
            type="primary",
            width="stretch"
        )
    
    if submitted:
        for branche, lr in loss_ratios.items():
            st.session_state[f'lr_saved_{branche}_{key_prefix}'] = lr
        st.session_state[f'loss_ratios_valides_{key_prefix}'] = loss_ratios
        st.success("✅ Loss Ratios validés!")
    
    return st.session_state.get(f'loss_ratios_valides_{key_prefix}', loss_ratios)


def _get_default_loss_ratios(branches: list[str]) -> dict:
    """Retourne les Loss Ratios par défaut."""
    return {branche: DEFAULT_LOSS_RATIO for branche in branches}


def is_loss_ratios_validated(key_prefix: str = "") -> bool:
    """Vérifie si les Loss Ratios ont été validés"""
    return f'loss_ratios_valides_{key_prefix}' in st.session_state
