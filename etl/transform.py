import pandas as pd
import numpy as np
from etl.extract import extract_charges_telephoniques, extract_charges_impression

# =============================================================================
# UTILITAIRES DE VALIDATION ET NETTOYAGE
# =============================================================================

def _valider_et_convertir_dates(df, col_date, nom_dataset=""):
    """Convertir colonne en datetime avec logging des erreurs."""
    print(f"[INFO] Validation dates {nom_dataset} - colonne '{col_date}'...")
    before = df[col_date].notna().sum()
    df[col_date] = pd.to_datetime(df[col_date], format='mixed', errors='coerce')
    after = df[col_date].notna().sum()
    lost = before - after
    if lost > 0:
        print(f"  ⚠️  {lost} dates invalides converties en NaT")
        print(f"  Lignes affectées (premiers exemples):")
        print(df[df[col_date].isna()].head(3))
    else:
        print(f"  ✓ Toutes les dates valides ({after} lignes)")
    return df


def _supprimer_doublons(df, subset_cols, nom_dataset=""):
    """Supprimer les doublons basés sur des colonnes spécifiques."""
    print(f"[INFO] Suppression doublons {nom_dataset}...")
    before = len(df)
    df = df.drop_duplicates(subset=subset_cols, keep='first')
    after = len(df)
    removed = before - after
    if removed > 0:
        print(f"  ⚠️  {removed} doublon(s) supprimé(s) ({removed/before*100:.1f}%)")
    else:
        print(f"  ✓ Aucun doublon détecté")
    return df


def _reset_ids_et_trier(df, col_date, col_id_existing, nom_dataset=""):
    """Trier par date et réassigner les IDs de 1 à n à la colonne ID existante."""
    print(f"[INFO] Réassignation IDs et tri {nom_dataset}...")
    df['DateValid'] = df[col_date].notna()
    invalid_count = (~df['DateValid']).sum()
    if invalid_count > 0:
        print(f"  ⚠️  {invalid_count} ligne(s) avec date manquante")
    df_valid = df[df['DateValid']].sort_values(by=col_date, ascending=True)
    df_invalid = df[~df['DateValid']]
    df = pd.concat([df_valid, df_invalid], ignore_index=True)
    # Réaffecter directement la colonne ID existante
    if col_id_existing in df.columns:
        df[col_id_existing] = range(1, len(df) + 1)
    print(f"  ✓ {len(df)} lignes conservées, IDs réassignés (1-{len(df)})")
    print(f"    - {len(df_valid)} lignes valides")
    print(f"    - {len(df_invalid)} lignes avec date invalide")
    return df


def _imputer_par_mapping(df, col_cible, col_source, exclude_values=None):
    """Imputer les valeurs manquantes de col_cible en se basant sur col_source."""
    condition = df[col_cible].notnull()
    if exclude_values:
        for val in exclude_values:
            condition = condition & (df[col_cible] != val)
    mapping = (
        df[condition]
        .drop_duplicates(subset=col_source)
        .set_index(col_source)[col_cible]
        .to_dict()
    )
    if exclude_values:
        mask = df[col_cible].isnull()
        for val in exclude_values:
            mask = mask | (df[col_cible] == val)
        df.loc[mask, col_cible] = df.loc[mask, col_source].map(mapping)
    else:
        df[col_cible] = df[col_cible].fillna(df[col_source].map(mapping))
    return df


def _forcer_colonnes_string(df, colonnes):
    """Forcer un sous-ensemble de colonnes en dtype string lorsqu'elles existent."""
    for col in colonnes:
        if col in df.columns:
            df[col] = df[col].astype('string')
    return df


# =============================================================================
# NORMALISATION DÉPARTEMENT
# =============================================================================

DEPARTEMENTS_VALIDES = [
    "PLPP", "LOGISTIQUE", "FINANCE", "EHS", "PRODUCTION A", "PRODUCTION B",
    "COSEE", "ACHAT", "ENGENIERIE", "IT", "OLS", "DIRECTION", "NYS", "TD", "QUALITE", "RH"
]


CODE_DEPARTEMENT_MAP = {
    "PLPP": "PLPP",
    "LOGISTIQUE": "LOG",
    "FINANCE": "FIN",
    "EHS": "EHS",
    "PRODUCTION A": "PROD-A",
    "PRODUCTION B": "PROD-B",
    "COSEE": "COSEE",
    "ACHAT": "ACH",
    "IT": "IT",
    "OLS": "OLS",
    "DIRECTION": "DIR",
    "NYS": "NYS",
    "TD": "TD",
    "ENGENIERIE": "ENG",
    "QUALITE": "QUA",
    "RH": "RH",
    "INCONNU": "INCONNU",
}


CODE_ROLE_MAP = {
    "LINE LEADER": "LL",
    "TECHNICIEN": "TECH",
    "MANAGER": "MGR",
    "SUPERVISEUR COMITE DIRECTION": "SUP-CD",
    "CENTRAL FUNCTION": "CF",
    "TEAM MANAGER": "TM",
    "SPECIALISTE": "SP",
    "ASSISTANTE DG": "ASS-DG",
    "HEAD": "HD",
    "SUPERVISEUR": "SUP",
    "CENTRAL FUNCTION MANAGER": "CFM",
}


def ajouter_code_departement(df, col_nom='NomDepartement'):
    """Ajouter la colonne CodeDepartement à partir de NomDepartement."""
    if col_nom not in df.columns:
        return df
    nom_norm = df[col_nom].astype('string').str.strip().str.upper()
    codes = nom_norm.map(CODE_DEPARTEMENT_MAP)
    missing = codes.isna().sum()
    if missing > 0:
        print(f"[INFO] CodeDepartement: {missing} valeur(s) non mappée(s) → 'INCONNU'")
        codes = codes.fillna('INCONNU')
    # Insérer/déplacer juste après NomDepartement
    if 'CodeDepartement' in df.columns:
        df = df.drop(columns=['CodeDepartement'])
    insert_at = df.columns.get_loc(col_nom) + 1
    df.insert(insert_at, 'CodeDepartement', codes)
    return df


def ajouter_code_role(df, col_nom='NomRole'):
    """Ajouter la colonne CodeRole à partir de NomRole."""
    if col_nom not in df.columns:
        return df
    nom_norm = df[col_nom].astype('string').str.strip().str.upper()
    codes = nom_norm.map(CODE_ROLE_MAP)
    missing = codes.isna().sum()
    if missing > 0:
        print(f"[INFO] CodeRole: {missing} valeur(s) non mappée(s) → 'INCONNU'")
        codes = codes.fillna('INCONNU')
    # Insérer/déplacer juste après NomRole
    if 'CodeRole' in df.columns:
        df = df.drop(columns=['CodeRole'])
    insert_at = df.columns.get_loc(col_nom) + 1
    df.insert(insert_at, 'CodeRole', codes)
    return df


def normaliser_nom_departement_telephoniques(df, col_date='DateOperation'):
    """Normaliser NomDepartement en propageant la dernière valeur valide du mois à tous les jours du mois."""
    if 'NomDepartement' not in df.columns or 'CodeEmployee' not in df.columns or col_date not in df.columns:
        return df
    print("[INFO] Normalisation NomDepartement (ChargesTelephoniques) - propagation dernière valeur valide du mois...")
    
    # Normaliser en majuscules et strip
    df['NomDepartement'] = df['NomDepartement'].str.strip().str.upper()
    
    # Extraire année et mois
    df['Annee'] = df[col_date].dt.year
    df['Mois'] = df[col_date].dt.month
    
    # Trier pour avoir les dates les plus récentes en premier
    df_sorted = df.sort_values(by=['CodeEmployee', 'Annee', 'Mois', col_date], ascending=[True, True, True, False])
    
    mapping = {}
    for (code_emp, annee, mois), group in df_sorted.groupby(['CodeEmployee', 'Annee', 'Mois']):
        for idx, row in group.iterrows():
            if pd.notna(row['NomDepartement']) and row['NomDepartement'] in DEPARTEMENTS_VALIDES:
                mapping[(code_emp, annee, mois)] = row['NomDepartement']
                break
    
    # Appliquer le mapping
    if mapping:
        for (code_emp, annee, mois), dept in mapping.items():
            mask = (df['CodeEmployee'] == code_emp) & (df['Annee'] == annee) & (df['Mois'] == mois)
            df.loc[mask, 'NomDepartement'] = dept
        print(f"  ✓ {len(mapping)} combinaison(s) traité(e)s")
    
    # Remplir les valeurs toujours invalides par 'INCONNU'
    remaining_invalid = df['NomDepartement'].isna() | (~df['NomDepartement'].isin(DEPARTEMENTS_VALIDES))
    others_count = remaining_invalid.sum()
    if others_count > 0:
        print(f"  ⚠️  {others_count} ligne(s) avec département invalide → 'INCONNU'")
        df.loc[remaining_invalid, 'NomDepartement'] = 'INCONNU'
    else:
        print(f"  ✓ Tous les départements valides")
    
    # Nettoyer les colonnes temporaires
    df = df.drop(columns=['Annee', 'Mois'])
    
    return df


def normaliser_nom_departement_impression(df):
    """Normaliser NomDepartement pour ChargesImpression sans fallback."""
    if 'NomDepartement' not in df.columns:
        return df
    print("[INFO] Normalisation NomDepartement (ChargesImpression)...")
    df['NomDepartement'] = df['NomDepartement'].str.strip().str.upper()
    invalid_mask = ~df['NomDepartement'].isin(DEPARTEMENTS_VALIDES)
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        print(f"  ⚠️  {invalid_count} département(s) invalide(s) → 'INCONNU'")
        df.loc[invalid_mask, 'NomDepartement'] = 'INCONNU'
    else:
        print(f"  ✓ Tous les départements valides")
    return df


def _propager_departement_consistent(df):
    """Attribuer le NomDepartement le plus courant par CodeEmployee."""
    print("[INFO] Uniformisation NomDepartement (le plus récurrent par CodeEmployee)...")
    mapping = {}
    for code_emp in df['CodeEmployee'].unique():
        subset = df[df['CodeEmployee'] == code_emp]
        valid_depts = subset[
            subset['NomDepartement'].notna()
            & (subset['NomDepartement'] != '')
            & (subset['NomDepartement'] != 'INCONNU')
        ]['NomDepartement']
        if len(valid_depts) > 0:
            most_common_dept = valid_depts.value_counts().idxmax()
            mapping[code_emp] = most_common_dept
    if mapping:
        for code_emp, dept in mapping.items():
            mask = df['CodeEmployee'] == code_emp
            df.loc[mask, 'NomDepartement'] = dept
    print(f"  ✓ {len(mapping)} CodeEmployee(s) ont un NomDepartement unique")
    return df


# =============================================================================
# TRANSFORMATION CHARGES TÉLÉPHONIQUES
# =============================================================================

def _corriger_types_telephoniques(ChargesTelephoniques):
    """Corriger les types de données de ChargesTelephoniques."""
    ChargesTelephoniques = _valider_et_convertir_dates(ChargesTelephoniques, 'DateOperation', 'ChargesTelephoniques')
    # Traiter seulement les colonnes qui existent
    cols_string = ['NomDepartement','CodeDepartement', 'NomRole','CodeRole','CodeEmployee', 'NumeroTelephone']
    cols_exist = [col for col in cols_string if col in ChargesTelephoniques.columns]
    for col in cols_exist:
        ChargesTelephoniques[col] = ChargesTelephoniques[col].astype('string')
    if 'NomRole' in ChargesTelephoniques.columns:
        ChargesTelephoniques['NomRole'] = _normaliser_nom_role(ChargesTelephoniques['NomRole'])
    if 'ForfaitTND' in ChargesTelephoniques.columns:
        # Remplir les NaN avec 0 avant la conversion en int64
        ChargesTelephoniques['ForfaitTND'] = ChargesTelephoniques['ForfaitTND'].fillna(0).astype('int64')
    return ChargesTelephoniques


def _normaliser_nom_role(serie):
    """Normaliser NomRole en majuscules et convertir les variantes NULL en INCONNU."""
    return (
        serie.astype('string')
        .replace({'NULL': pd.NA, 'null': pd.NA, 'Null': pd.NA})
        .str.strip()
        .str.upper()
        .fillna('INCONNU')
    )


FORFAIT_TO_NOMROLE_MAP = {
    0: 'HEAD',
    20: 'LINE LEADER',
    25: 'TECHNICIEN',
    40: 'SPECIALISTE',
    50: 'CENTRAL FUNCTION',
    70: 'SUPERVISEUR COMITE DIRECTION',
    80: 'TEAM MANAGER',
    100: 'MANAGER',
}


def _imputer_nomrole_par_forfait(ChargesTelephoniques):
    """Imputer NomRole à partir de ForfaitTND pour les forfaits explicitement connus."""
    if 'NomRole' not in ChargesTelephoniques.columns or 'ForfaitTND' not in ChargesTelephoniques.columns:
        return ChargesTelephoniques
    print("[INFO] Imputation NomRole par ForfaitTND...")
    ChargesTelephoniques['NomRole'] = _normaliser_nom_role(ChargesTelephoniques['NomRole'])
    mask = ChargesTelephoniques['NomRole'].eq('INCONNU')
    count_inconnu = mask.sum()
    print(f"  Lignes avec NomRole == 'INCONNU' : {count_inconnu}")
    
    if mask.any():
        mapped_roles = ChargesTelephoniques.loc[mask, 'ForfaitTND'].map(FORFAIT_TO_NOMROLE_MAP)
        valid_index = mapped_roles.dropna().index
        count_mapped = len(valid_index)
        print(f"  Forfaits mappés : {count_mapped}")
        print(f"  Distribution des forfaits non mappés :")
        unmapped_forfaits = ChargesTelephoniques.loc[mask & ~ChargesTelephoniques.index.isin(valid_index), 'ForfaitTND'].value_counts()
        for forfait, count in unmapped_forfaits.items():
            print(f"    - ForfaitTND {forfait}: {count} lignes")
        
        if len(valid_index) > 0:
            ChargesTelephoniques.loc[valid_index, 'NomRole'] = mapped_roles.loc[valid_index]
            print(f"  ✓ {count_mapped} NomRole imputés par ForfaitTND")
    
    return ChargesTelephoniques


def _imputer_valeurs_telephoniques(ChargesTelephoniques):
    """Imputer toutes les valeurs manquantes de ChargesTelephoniques."""
    # CodeDepartement et NomResponsable n'existent pas dans les données
    if 'NomRole' in ChargesTelephoniques.columns and 'CodeEmployee' in ChargesTelephoniques.columns:
        ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'NomRole', 'CodeEmployee')
        ChargesTelephoniques['NomRole'] = _normaliser_nom_role(ChargesTelephoniques['NomRole'])
    ChargesTelephoniques = _imputer_nomrole_par_forfait(ChargesTelephoniques)
    if 'ForfaitTND' in ChargesTelephoniques.columns and 'NomRole' in ChargesTelephoniques.columns:
        ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'ForfaitTND', 'NomRole')
    return ChargesTelephoniques




def _nettoyer_code_employee(ChargesTelephoniques):
    """Formater les CodeEmployee en format standardisé : YAZ + nombre."""
    if 'CodeEmployee' not in ChargesTelephoniques.columns:
        return ChargesTelephoniques
    print("[INFO] Normalisation CodeEmployee (format: YAZ+nombre ou INCONNU)...")
    def formater_code(code):
        if pd.isna(code):
            return "INCONNU"
        code_str = str(code).upper().strip()
        digits = ''.join(c for c in code_str if c.isdigit())
        return f"YAZ{digits}" if digits else "INCONNU"
    ChargesTelephoniques['CodeEmployee'] = ChargesTelephoniques['CodeEmployee'].apply(formater_code)
    print("  ✓ CodeEmployee reformatés")
    return ChargesTelephoniques


def _corriger_forfaits(ChargesTelephoniques):
    """Corriger les ForfaitTND par NomRole."""
    if 'ForfaitTND' not in ChargesTelephoniques.columns or 'NomRole' not in ChargesTelephoniques.columns:
        return ChargesTelephoniques
    mapping_nomrole_forfait = {
        "LINE LEADER": 20, "TECHNICIEN": 25, "MANAGER": 100, "SUPERVISEUR COMITE DIRECTION": 70,
        "CENTRAL FUNCTION": 50, "TEAM MANAGER": 80, "SPECIALISTE": 40, "ASSISTANTE DG": 80,
        "HEAD": 0, "SUPERVISEUR": 50, "CENTRAL FUNCTION MANAGER": 100
    }
    forfaits_autorises = {0, 20, 25, 40, 50, 70, 80, 100}
    nomrole_norm = _normaliser_nom_role(ChargesTelephoniques['NomRole'])
    for role, forfait in mapping_nomrole_forfait.items():
        ChargesTelephoniques.loc[nomrole_norm == role, 'ForfaitTND'] = forfait
    ChargesTelephoniques.loc[~ChargesTelephoniques['ForfaitTND'].isin(forfaits_autorises), 'ForfaitTND'] = 0
    return ChargesTelephoniques




def _propager_role_tel_dernier_mois(df, col_date='DateOperation'):
    """Propager NomRole et NumeroTelephone du dernier jour du mois à tous les jours du mois."""
    if 'CodeEmployee' not in df.columns or 'NomRole' not in df.columns or 'NumeroTelephone' not in df.columns:
        return df
    print("[INFO] Propagation NomRole et NumeroTelephone (dernier jour du mois → tous les jours du mois)...")
    df['Annee'] = df[col_date].dt.year
    df['Mois'] = df[col_date].dt.month
    df_sorted = df.sort_values(by=['CodeEmployee', 'Annee', 'Mois', col_date], ascending=[True, True, True, False])
    mapping = {}
    for (code_emp, annee, mois), group in df_sorted.groupby(['CodeEmployee', 'Annee', 'Mois']):
        # Chercher NomRole valide (pas NaN, pas NULL, pas chaîne NULL, pas vide)
        nom_role_found = None
        for idx, row in group.iterrows():
            nom_role = row['NomRole']
            if pd.notna(nom_role):
                nom_role_normalized = str(nom_role).strip().upper()
                # Exclure les variantes de "NULL", "NAN", chaînes vides, et "INCONNU"
                if nom_role_normalized not in {'NULL', 'NAN', '', 'INCONNU'}:
                    nom_role_found = nom_role_normalized
                    break
        
        # Chercher NumeroTelephone valide (peu importe NomRole)
        numero_tel_found = None
        for idx, row in group.iterrows():
            if pd.notna(row['NumeroTelephone']):
                numero_tel_found = row['NumeroTelephone']
                break
        
        # Enregistrer le mapping si au moins un des deux est trouvé
        if nom_role_found or numero_tel_found:
            mapping[(code_emp, annee, mois)] = {
                'NomRole': nom_role_found,
                'NumeroTelephone': numero_tel_found
            }
    
    # Appliquer le mapping
    if mapping:
        for (code_emp, annee, mois), values in mapping.items():
            mask = (df['CodeEmployee'] == code_emp) & (df['Annee'] == annee) & (df['Mois'] == mois)
            if values['NomRole']:
                df.loc[mask, 'NomRole'] = values['NomRole']
            if values['NumeroTelephone']:
                df.loc[mask, 'NumeroTelephone'] = values['NumeroTelephone']
        print(f"  ✓ {len(mapping)} combinaison(s) traité(e)s")
    
    df = df.drop(columns=['Annee', 'Mois'])
    df['NomRole'] = _normaliser_nom_role(df['NomRole'])
    return df


def transform_charges_telephoniques(ChargesTelephoniques):
    """Pipeline complet de transformation pour ChargesTelephoniques."""
    # IMPORTANT: Faire la propagation AVANT la normalisation qui convertit NaN en INCONNU
    ChargesTelephoniques = _valider_et_convertir_dates(ChargesTelephoniques, 'DateOperation', 'ChargesTelephoniques')
    ChargesTelephoniques = _propager_role_tel_dernier_mois(ChargesTelephoniques, 'DateOperation')
    
    # Puis appliquer les transformations normales
    ChargesTelephoniques = _corriger_types_telephoniques(ChargesTelephoniques)
    ChargesTelephoniques = _imputer_valeurs_telephoniques(ChargesTelephoniques)
    ChargesTelephoniques = normaliser_nom_departement_telephoniques(ChargesTelephoniques)
    ChargesTelephoniques = _nettoyer_code_employee(ChargesTelephoniques)
    ChargesTelephoniques = _propager_departement_consistent(ChargesTelephoniques)
    ChargesTelephoniques = _corriger_forfaits(ChargesTelephoniques)
    ChargesTelephoniques = _imputer_nomrole_par_forfait(ChargesTelephoniques)
    ChargesTelephoniques = ajouter_code_departement(ChargesTelephoniques, 'NomDepartement')
    ChargesTelephoniques = ajouter_code_role(ChargesTelephoniques, 'NomRole')
    cols_doublon = [col for col in ChargesTelephoniques.columns if col != 'TelephoniqueID']
    ChargesTelephoniques = _supprimer_doublons(ChargesTelephoniques, cols_doublon, 'ChargesTelephoniques')
    ChargesTelephoniques = _reset_ids_et_trier(ChargesTelephoniques, 'DateOperation', 'TelephoniqueID', 'ChargesTelephoniques')
    ChargesTelephoniques = _forcer_colonnes_string(ChargesTelephoniques, ['CodeDepartement', 'CodeRole', 'CodeEmployee'])
    return ChargesTelephoniques


# =============================================================================
# TRANSFORMATION CHARGES IMPRESSION
# =============================================================================

def _corriger_types_impression(ChargesImpression):
    """Corriger les types de données de ChargesImpression."""
    ChargesImpression = _valider_et_convertir_dates(ChargesImpression, 'DateImpression', 'ChargesImpression')
    cols_string = ['NomDepartement','CodeDepartement', 'CouleurImpression', 'TypeImpression', 'FormatPapier']
    cols_exist = [col for col in cols_string if col in ChargesImpression.columns]
    for col in cols_exist:
        ChargesImpression[col] = ChargesImpression[col].astype('string')
    if 'NbPages' in ChargesImpression.columns:
        ChargesImpression['NbPages'] = ChargesImpression['NbPages'].fillna(0).astype('int64')
    if 'CoutUnitaire' in ChargesImpression.columns:
        ChargesImpression['CoutUnitaire'] = ChargesImpression['CoutUnitaire'].fillna(0.0).astype('float64')
    return ChargesImpression


def _imputer_valeurs_impression(ChargesImpression):
    """Imputer toutes les valeurs manquantes de ChargesImpression."""
    # Cette fonction peut rester vide puisque les colonnes réelles n'ont pas de mappings disponibles
    # Les transformations spécifiques seront faites dans _corriger_types_impression
    return ChargesImpression


def _valider_type_impression(ChargesImpression):
    """Valider TypeImpression : A3-COULEUR, A4-COULEUR, A3-NB, A4-NB, sinon INCONNU."""
    if 'TypeImpression' not in ChargesImpression.columns:
        return ChargesImpression
    types_valides = ["A3-COULEUR", "A4-COULEUR", "A3-NB", "A4-NB"]
    print("[INFO] Validation TypeImpression...")
    ChargesImpression['TypeImpression'] = ChargesImpression['TypeImpression'].str.strip().str.upper()
    invalid_mask = ~ChargesImpression['TypeImpression'].isin(types_valides)
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        print(f"  ⚠️  {invalid_count} type(s) impression invalide(s) → 'INCONNU'")
        ChargesImpression.loc[invalid_mask, 'TypeImpression'] = 'INCONNU'
    else:
        print(f"  ✓ Tous les types impression valides")
    return ChargesImpression


def _extraire_couleur_et_format(ChargesImpression):
    """Extraire CouleurImpression et FormatPapier à partir de TypeImpression."""
    if 'TypeImpression' not in ChargesImpression.columns:
        return ChargesImpression
    print("[INFO] Extraction CouleurImpression et FormatPapier...")
    def extraire_info(type_impr):
        if pd.isna(type_impr):
            return 'INCONNU', 'INCONNU'
        type_impr = str(type_impr).upper().strip()
        couleur, format_papier = 'INCONNU', 'INCONNU'
        if 'A3' in type_impr:
            format_papier = 'A3'
        elif 'A4' in type_impr:
            format_papier = 'A4'
        if 'COULEUR' in type_impr:
            couleur = 'COULEUR'
        elif 'NB' in type_impr:
            couleur = 'NOIR ET BLANC'
        return format_papier, couleur
    result = ChargesImpression['TypeImpression'].apply(extraire_info)
    ChargesImpression['FormatPapier'] = result.apply(lambda x: x[0])
    ChargesImpression['CouleurImpression'] = result.apply(lambda x: x[1])
    ChargesImpression = _forcer_colonnes_string(ChargesImpression, ['FormatPapier', 'CouleurImpression'])
    print(f"  ✓ Extraction complète")
    return ChargesImpression


def _corriger_cout_unitaire(ChargesImpression):
    """Corriger CoutUnitaire selon TypeImpression."""
    if 'TypeImpression' not in ChargesImpression.columns or 'CoutUnitaire' not in ChargesImpression.columns:
        return ChargesImpression
    mapping_format_cout = {
        "A4-NB": 0.026, "A3-COULEUR": 0.313, "A3-NB": 0.052, "A4-COULEUR": 0.156
    }
    print("[INFO] Correction CoutUnitaire...")
    type_norm = ChargesImpression['TypeImpression'].str.upper().str.strip()
    for format_type, cout in mapping_format_cout.items():
        ChargesImpression.loc[type_norm == format_type, 'CoutUnitaire'] = cout
    print(f"  ✓ CoutUnitaire corrigés")
    return ChargesImpression


def _corriger_nb_pages(ChargesImpression):
    """Rendre positives les valeurs de NbPages et remplir les vides avec 0."""
    if 'NbPages' not in ChargesImpression.columns:
        return ChargesImpression
    print("[INFO] Correction NbPages (positif, NA → 0)...")
    ChargesImpression['NbPages'] = ChargesImpression['NbPages'].fillna(0)
    ChargesImpression['NbPages'] = ChargesImpression['NbPages'].abs()
    ChargesImpression['NbPages'] = ChargesImpression['NbPages'].astype('int')
    print(f"  ✓ NbPages corrigés")
    return ChargesImpression


def transform_charges_impression(ChargesImpression):
    """Pipeline complet de transformation pour ChargesImpression."""
    ChargesImpression = _corriger_types_impression(ChargesImpression)
    ChargesImpression = normaliser_nom_departement_impression(ChargesImpression)
    ChargesImpression = ajouter_code_departement(ChargesImpression, 'NomDepartement')
    ChargesImpression = _valider_type_impression(ChargesImpression)
    ChargesImpression = _extraire_couleur_et_format(ChargesImpression)
    ChargesImpression = _corriger_cout_unitaire(ChargesImpression)
    ChargesImpression = _imputer_valeurs_impression(ChargesImpression)
    ChargesImpression = _corriger_nb_pages(ChargesImpression)
    # Pas de suppression de doublons : un même employé peut faire deux opérations identiques le même jour
    ChargesImpression = _reset_ids_et_trier(ChargesImpression, 'DateImpression', 'ImpressionID', 'ChargesImpression')
    ChargesImpression = _forcer_colonnes_string(ChargesImpression, ['CodeDepartement', 'FormatPapier', 'CouleurImpression'])
    return ChargesImpression


