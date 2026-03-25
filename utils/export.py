"""
Module d'export Excel et PDF
"""
import io
import pandas as pd
import polars as pl
from fpdf import FPDF
from datetime import date

def create_full_report(items: list) -> bytes:
    """
    Génère un rapport Excel complet avec tous les tableaux et commentaires sur UNE SEULE feuille.
    
    Args:
        items: Liste de tuples [("Titre", DataFrame ou str), ...]
               Si le 2ème élément est un str, c'est un commentaire.
    
    Returns:
        bytes: Le contenu du fichier Excel
    """
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        sheet_name = "Rapport_Complet"
        start_row = 0
        
        for title, data in items:
            
            # Écrire le titre de la section
            pd.DataFrame([title]).to_excel(
                writer, 
                sheet_name=sheet_name, 
                startrow=start_row, 
                startcol=0, 
                index=False, 
                header=False
            )
            # Appliquer un style gras/grand au titre serait bien mais openpyxl direct est complexe ici à mélanger avec pandas writer.
            # On reste simple.
            
            current_row = start_row + 1
            
            if isinstance(data, str):
                # C'est un commentaire
                # On l'écrit comme une cellule de texte
                if data.strip(): # Si commentaire non vide
                    pd.DataFrame([data]).to_excel(
                        writer,
                        sheet_name=sheet_name,
                        startrow=current_row,
                        startcol=0,
                        index=False,
                        header=False
                    )
                    start_row = current_row + 2 # +1 ligne data + 1 marge
                else:
                    start_row = current_row + 1 # Juste marge
            
            else:
                # C'est un DataFrame (Tableau)
                if isinstance(data, pl.DataFrame):
                    pdf = data.to_pandas()
                else:
                    pdf = data
                
                pdf.to_excel(
                    writer, 
                    sheet_name=sheet_name, 
                    startrow=current_row + 1, # +1 pour laisser espace après titre
                    index=False
                )
                
                # +1 titre +1 espace +1 header df + len(df) + 2 marge
                start_row = current_row + 1 + 1 + len(pdf) + 3
            
        # Ajustement auto des colonnes
        if sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for col in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L']:
                worksheet.column_dimensions[col].width = 25
                
    return output.getvalue()


# ═══════════════════════════════════════════════════════════════════════
#  EXPORT PDF
# ═══════════════════════════════════════════════════════════════════════

# Couleurs — Charte Leadway (orange dominant)
_ORANGE_FONCE = (230, 126, 34)   # #E67E22 - titres, en-têtes, bandeaux
_ORANGE_CLAIR = (253, 235, 220)  # fond alterné tableau
_GRIS_CLAIR = (245, 245, 245)    # fond commentaire
_BLANC = (255, 255, 255)
_NOIR = (33, 33, 33)
_GRIS_TEXTE = (80, 80, 80)


def _sanitize_text(text: str) -> str:
    """Remplace les caractères Unicode non supportés par les polices core PDF."""
    replacements = {
        "\u2014": "-",   # em-dash
        "\u2013": "-",   # en-dash
        "\u2018": "'",   # left single quote
        "\u2019": "'",   # right single quote
        "\u201c": '"',   # left double quote
        "\u201d": '"',   # right double quote
        "\u2026": "...", # ellipsis
        "\u2022": "-",   # bullet
        "\u00b7": "-",   # middle dot
        "\u2265": ">=",  # >=
        "\u2264": "<=",  # <=
        "\u00d7": "x",   # multiplication sign
        "\u2192": "->",  # right arrow
        "\u00e9": "e",   # é
        "\u00e8": "e",   # è
        "\u00ea": "e",   # ê
        "\u00e0": "a",   # à
        "\u00f4": "o",   # ô
        "\u00e7": "c",   # ç
        "\u00fb": "u",   # û
        "\u00ee": "i",   # î
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    # Fallback : remplacer tout caractère non latin-1 restant
    try:
        text.encode("latin-1")
    except UnicodeEncodeError:
        text = text.encode("latin-1", errors="replace").decode("latin-1")
    return text


def _format_number_pdf(val) -> str:
    """Formate un nombre pour le PDF (séparateurs de milliers, 2 décimales)."""
    if val is None:
        return ""
    try:
        f = float(val)
        if abs(f) >= 1:
            return f"{f:,.2f}".replace(",", " ")
        elif f == 0:
            return "0,00"
        else:
            return f"{f:.4f}".replace(".", ",")
    except (ValueError, TypeError):
        return str(val)


class _ActuarialPDF(FPDF):
    """PDF personnalisé avec en-tête et pied de page professionnels."""

    def __init__(self, date_eval: str = "", **kwargs):
        super().__init__(orientation="L", unit="mm", format="A4", **kwargs)
        self.date_eval = date_eval
        self._section_number = 0
        self.set_auto_page_break(auto=True, margin=20)

    def cell(self, w=None, h=None, text="", *args, **kwargs):
        """Surcharge cell pour sanitiser automatiquement le texte."""
        return super().cell(w, h, _sanitize_text(str(text)) if text else "", *args, **kwargs)

    def multi_cell(self, w, h=None, text="", *args, **kwargs):
        """Surcharge multi_cell pour sanitiser automatiquement le texte."""
        return super().multi_cell(w, h, _sanitize_text(str(text)) if text else "", *args, **kwargs)

    # ── En-tête ──────────────────────────────────────────────────────
    def header(self):
        if self.page_no() == 1:
            return  # Page de garde gérée séparément
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*_ORANGE_FONCE)
        self.cell(0, 6, "Rapport Actuariel — PREC / PA / PSAP", align="L")
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*_GRIS_TEXTE)
        self.cell(0, 6, f"Date d'évaluation : {self.date_eval}", align="R", new_x="LMARGIN", new_y="NEXT")
        # Ligne de séparation
        self.set_draw_color(*_ORANGE_FONCE)
        self.set_line_width(0.4)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    # ── Pied de page ─────────────────────────────────────────────────
    def footer(self):
        if self.page_no() == 1:
            return
        self.set_y(-15)
        self.set_draw_color(*_ORANGE_FONCE)
        self.set_line_width(0.3)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(2)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*_GRIS_TEXTE)
        self.cell(0, 5, "Document généré automatiquement — Dashboard Actuariat", align="L")
        self.cell(0, 5, f"Page {self.page_no()} / {{nb}}", align="R")

    # ── Page de garde ────────────────────────────────────────────────
    def cover_page(self):
        self.add_page()
        # Fond bandeau supérieur
        self.set_fill_color(*_ORANGE_FONCE)
        self.rect(0, 0, self.w, 85, "F")

        # Titre principal
        self.set_y(22)
        self.set_font("Helvetica", "B", 32)
        self.set_text_color(*_BLANC)
        self.cell(0, 14, "RAPPORT ACTUARIEL", align="C", new_x="LMARGIN", new_y="NEXT")

        self.set_font("Helvetica", "", 18)
        self.cell(0, 10, "Provisions Techniques — PREC / PA / PSAP", align="C", new_x="LMARGIN", new_y="NEXT")

        self.ln(8)
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, f"Date d'évaluation : {self.date_eval}", align="C", new_x="LMARGIN", new_y="NEXT")

        # Séparateur décoratif
        self.ln(30)
        self.set_draw_color(*_ORANGE_FONCE)
        self.set_line_width(0.8)
        cx = self.w / 2
        self.line(cx - 40, self.get_y(), cx + 40, self.get_y())

        # Infos
        self.ln(12)
        self.set_font("Helvetica", "", 12)
        self.set_text_color(*_NOIR)
        self.cell(0, 8, "Direction Technique — Département Actuariat", align="C", new_x="LMARGIN", new_y="NEXT")
        self.set_font("Helvetica", "I", 10)
        self.set_text_color(*_GRIS_TEXTE)
        self.cell(0, 8, "Analyse de la production, des provisions pour risques en cours", align="C", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 8, "et des provisions pour sinistres à payer", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── Sommaire ─────────────────────────────────────────────────────
    def table_of_contents(self, sections: list[str]):
        self.add_page()
        self.set_font("Helvetica", "B", 20)
        self.set_text_color(*_ORANGE_FONCE)
        self.cell(0, 12, "SOMMAIRE", align="L", new_x="LMARGIN", new_y="NEXT")
        self.ln(6)

        self.set_draw_color(*_ORANGE_FONCE)
        self.set_line_width(0.5)
        self.line(self.l_margin, self.get_y(), self.l_margin + 50, self.get_y())
        self.ln(8)

        for i, section in enumerate(sections, 1):
            self.set_font("Helvetica", "B", 12)
            self.set_text_color(*_ORANGE_FONCE)
            self.cell(10, 9, f"{i}.", align="R")
            self.set_font("Helvetica", "", 12)
            self.set_text_color(*_NOIR)
            self.cell(0, 9, f"  {section}", new_x="LMARGIN", new_y="NEXT")
            # Pointillés
            self.set_draw_color(200, 200, 200)
            self.set_line_width(0.1)
            y = self.get_y() - 1
            self.dashed_line(self.l_margin + 12, y, self.w - self.r_margin, y, 1, 2)

    # ── Titre de section ─────────────────────────────────────────────
    def section_title(self, title: str):
        self._section_number += 1
        # Saut de page si pas assez de place
        if self.get_y() > self.h - 50:
            self.add_page()

        self.ln(4)
        # Bandeau coloré
        self.set_fill_color(*_ORANGE_FONCE)
        self.set_text_color(*_BLANC)
        self.set_font("Helvetica", "B", 13)
        self.cell(0, 10, f"  {self._section_number}. {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(3)
        self.set_text_color(*_NOIR)

    # ── Bloc commentaire ─────────────────────────────────────────────
    def comment_block(self, text: str):
        if not text or not text.strip():
            return
        self.set_fill_color(*_GRIS_CLAIR)
        self.set_text_color(*_GRIS_TEXTE)
        self.set_font("Helvetica", "I", 8.5)
        # Bordure gauche bleue
        x0 = self.l_margin
        y0 = self.get_y()
        # Écrire le texte dans une cellule multi-ligne
        self.set_x(x0 + 4)
        w = self.w - self.l_margin - self.r_margin - 4
        self.multi_cell(w, 4.5, text.strip(), fill=True)
        y1 = self.get_y()
        # Trait bleu à gauche
        self.set_draw_color(*_ORANGE_FONCE)
        self.set_line_width(1.2)
        self.line(x0, y0, x0, y1)
        self.ln(3)
        self.set_text_color(*_NOIR)

    # ── Sous-titre ───────────────────────────────────────────────────
    def sub_title(self, title: str):
        if self.get_y() > self.h - 35:
            self.add_page()
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*_ORANGE_FONCE)
        self.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(*_NOIR)
        self.ln(1)

    # ── Tableau de données ───────────────────────────────────────────
    def data_table(self, df):
        """Dessine un tableau Polars/Pandas formaté."""
        if isinstance(df, pl.DataFrame):
            pdf_df = df.to_pandas()
        else:
            pdf_df = df.copy()

        cols = list(pdf_df.columns)
        n_cols = len(cols)
        if n_cols == 0:
            return

        # Calculer les largeurs de colonnes
        avail_w = self.w - self.l_margin - self.r_margin
        first_col_w = min(55, avail_w * 0.22)
        remaining_w = avail_w - first_col_w
        other_col_w = remaining_w / max(1, n_cols - 1) if n_cols > 1 else avail_w

        col_widths = [first_col_w] + [other_col_w] * (n_cols - 1)

        row_h = 7

        # Vérifier s'il reste assez de place (en-tête + au moins 3 lignes)
        needed = row_h * min(4, len(pdf_df) + 1)
        if self.get_y() + needed > self.h - 20:
            self.add_page()

        # ── En-tête ──
        self.set_fill_color(*_ORANGE_FONCE)
        self.set_text_color(*_BLANC)
        self.set_font("Helvetica", "B", 7.5)
        self.set_draw_color(*_BLANC)

        for i, col in enumerate(cols):
            align = "L" if i == 0 else "R"
            self.cell(col_widths[i], row_h, f" {col} ", border=1, fill=True, align=align)
        self.ln()

        # ── Lignes de données ──
        self.set_font("Helvetica", "", 7.5)
        self.set_draw_color(200, 200, 200)

        for row_idx, (_, row) in enumerate(pdf_df.iterrows()):
            is_total = False
            if len(cols) > 0:
                first_val = str(row.iloc[0]).strip().upper()
                is_total = first_val == "TOTAL"

            # Alternance de couleurs
            if is_total:
                self.set_fill_color(*_ORANGE_FONCE)
                self.set_text_color(*_BLANC)
                self.set_font("Helvetica", "B", 7.5)
            elif row_idx % 2 == 0:
                self.set_fill_color(*_BLANC)
                self.set_text_color(*_NOIR)
                self.set_font("Helvetica", "", 7.5)
            else:
                self.set_fill_color(*_ORANGE_CLAIR)
                self.set_text_color(*_NOIR)
                self.set_font("Helvetica", "", 7.5)

            for i, col in enumerate(cols):
                val = row[col]
                align = "L" if i == 0 else "R"
                # Formatage
                if i == 0:
                    display = str(val) if val is not None else ""
                else:
                    display = _format_number_pdf(val)
                self.cell(col_widths[i], row_h, f" {display} ", border=1, fill=True, align=align)
            self.ln()

            # Saut de page si nécessaire au milieu du tableau
            if self.get_y() > self.h - 22:
                self.add_page()
                # Ré-afficher l'en-tête
                self.set_fill_color(*_ORANGE_FONCE)
                self.set_text_color(*_BLANC)
                self.set_font("Helvetica", "B", 7.5)
                self.set_draw_color(*_BLANC)
                for i, col_name in enumerate(cols):
                    align = "L" if i == 0 else "R"
                    self.cell(col_widths[i], row_h, f" {col_name} ", border=1, fill=True, align=align)
                self.ln()
                self.set_draw_color(200, 200, 200)

        self.set_text_color(*_NOIR)
        self.ln(4)


def create_full_report_pdf(items: list, date_eval: str = "") -> bytes:
    """
    Génère un rapport PDF complet et professionnel.
    
    Args:
        items: Liste de tuples [("Titre", DataFrame ou str), ...]
        date_eval: Date d'évaluation (pour l'en-tête)
    
    Returns:
        bytes: Le contenu du fichier PDF
    """
    pdf = _ActuarialPDF(date_eval=str(date_eval))
    pdf.alias_nb_pages()

    # ── Page de garde ──
    pdf.cover_page()

    # ── Sommaire ──
    # Extraire les titres de sections (ceux qui commencent par ---)
    section_titles = []
    for title, _ in items:
        if title.startswith("---") and title.endswith("---"):
            clean = title.strip("- ").strip()
            section_titles.append(clean)
    pdf.table_of_contents(section_titles)

    # ── Contenu ──
    pdf.add_page()

    for title, data in items:
        if title.startswith("---") and title.endswith("---"):
            # Titre de section principale + commentaire associé
            clean = title.strip("- ").strip()
            pdf.section_title(clean)
            if isinstance(data, str) and data.strip():
                pdf.comment_block(data)

        elif isinstance(data, str):
            # Bloc commentaire orphelin
            pdf.comment_block(data)

        else:
            # Sous-titre + tableau
            pdf.sub_title(title)
            if isinstance(data, pl.DataFrame):
                pdf.data_table(data)
            elif isinstance(data, pd.DataFrame):
                pdf.data_table(data)

    # ── Génération ──
    output = io.BytesIO()
    output.write(pdf.output())
    return output.getvalue()
