import pandas as pd
from etl.extract import extract_charges_telephoniques, extract_charges_impression

# Fonctions communes de transformation

def _valider_et_convertir_dates(df, col_date, nom_dataset=""):
    # Convertir colonne en datetime avec logging des erreurs.
    print(f"[INFO] Validation dates {nom_dataset} - colonne '{col_date}'...")
    before = df[col_date].notna().sum()
    # mixed  Accepte différents formats de dates
    # errors = Les dates invalides deviennent NaT (Not a Time) au lieu de lever une erreur
    df[col_date] = pd.to_datetime(df[col_date], format='mixed', errors='coerce')
    after = df[col_date].notna().sum()
    lost = before - after
    if lost > 0:
        print(f"  ⚠️  {lost} dates invalides converties en NaT")
    else:
        print(f"  ✓ Toutes les dates valides ({after} lignes)")
    return df

def ajouter_code_departement(df, col_nom='NomDepartement'):
    # Ajouter la colonne CodeDepartement à partir de NomDepartement.
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

def _forcer_colonnes_string(df, colonnes):
    # Forcer un sous-ensemble de colonnes en dtype string lorsqu'elles existent.
    for col in colonnes:
        if col in df.columns:
            df[col] = df[col].astype('string')
    return df

# --------------------------------------------------------
# Trandformation Charges Téléphoniques


def _corriger_types_telephoniques(ChargesTelephoniques):
    # Corriger les types de données de ChargesTelephoniques.
    ChargesTelephoniques = _valider_et_convertir_dates(ChargesTelephoniques, 'DateOperation', 'ChargesTelephoniques')
    # Traiter seulement les colonnes qui existent
    cols_string = ['NomDepartement', 'NomRole','CodeEmployee', 'NumeroTelephone']
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
    # Normaliser NomRole en majuscules et convertir les variantes NULL en INCONNU.
    return (
        serie.astype('string')
        .replace({'NULL': pd.NA, 'null': pd.NA, 'Null': pd.NA})
        .str.strip()
        .str.upper()
    )

def _supprimer_doublons(df, subset_cols, nom_dataset=""):
    # Supprimer les doublons basés sur des colonnes spécifiques.
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

def _nettoyer_code_employee(ChargesTelephoniques):
    # Formater les CodeEmployee en format standardisé : YAZ + nombre.
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


def normaliser_nom_departement_telephoniques(df, col_date='DateOperation'):
    if 'NomDepartement' not in df.columns or 'CodeEmployee' not in df.columns or col_date not in df.columns:
        return df
    print("[INFO] Normalisation NomDepartement - propagation inter-mois (valeur la plus récente)...")

    df['NomDepartement'] = df['NomDepartement'].str.strip().str.upper()

    # Trier par date décroissante → première valeur valide = la plus récente
    df_sorted = df.sort_values(by=[col_date], ascending=False)

    mapping = {}
    for code_emp, group in df_sorted.groupby('CodeEmployee'):
        depts_valides = group.loc[group['NomDepartement'].isin(DEPARTEMENTS_VALIDES), 'NomDepartement']
        if len(depts_valides) > 0:
            mapping[code_emp] = depts_valides.iloc[0]  # ← le plus récent

    # Appliquer uniquement sur les lignes invalides
    invalid_mask = df['NomDepartement'].isna() | (~df['NomDepartement'].isin(DEPARTEMENTS_VALIDES))
    df.loc[invalid_mask, 'NomDepartement'] = df.loc[invalid_mask, 'CodeEmployee'].map(mapping)

    # Fallback final
    remaining = df['NomDepartement'].isna() | (~df['NomDepartement'].isin(DEPARTEMENTS_VALIDES))
    count = remaining.sum()
    if count > 0:
        print(f"  ⚠️  {count} ligne(s) sans département valide → 'INCONNU'")
        df.loc[remaining, 'NomDepartement'] = 'INCONNU'
    else:
        print(f"  ✓ Tous les départements valides")

    return df

def _propager_role_tel_dernier_mois(df, col_date='DateOperation'):
    if 'CodeEmployee' not in df.columns or 'NomRole' not in df.columns or 'NumeroTelephone' not in df.columns:
        return df
    print("[INFO] Propagation NomRole et NumeroTelephone inter-mois (valeur la plus récente)...")

    df['NomRole'] = _normaliser_nom_role(df['NomRole'])

    # Trier par date décroissante → première valeur valide = la plus récente
    df_sorted = df.sort_values(by=[col_date], ascending=False)

    roles_valides_set = set(CODE_ROLE_MAP.keys())
    mapping_role = {}
    mapping_tel = {}

    for code_emp, group in df_sorted.groupby('CodeEmployee'):

        # NomRole → le plus récent valide
        roles_valides = group.loc[group['NomRole'].isin(roles_valides_set), 'NomRole']
        if len(roles_valides) > 0:
            mapping_role[code_emp] = roles_valides.iloc[0]

        # NumeroTelephone → le plus récent non nul
        tels_valides = group.loc[group['NumeroTelephone'].notna(), 'NumeroTelephone']
        if len(tels_valides) > 0:
            mapping_tel[code_emp] = tels_valides.iloc[0]

    # Appliquer NomRole uniquement sur les lignes invalides
    if mapping_role:
        invalid_mask = df['NomRole'].isna() | (~df['NomRole'].isin(roles_valides_set))
        df.loc[invalid_mask, 'NomRole'] = df.loc[invalid_mask, 'CodeEmployee'].map(mapping_role)

    # Appliquer NumeroTelephone uniquement sur les lignes nulles
    if mapping_tel:
        invalid_mask = df['NumeroTelephone'].isna()
        df.loc[invalid_mask, 'NumeroTelephone'] = df.loc[invalid_mask, 'CodeEmployee'].map(mapping_tel)

    # Log restants
    remaining_role = df['NomRole'].isna() | (~df['NomRole'].isin(roles_valides_set))
    if remaining_role.sum() > 0:
        print(f"  ⚠️  {remaining_role.sum()} NomRole toujours manquants → sera traité par ForfaitTND")
    else:
        print(f"  ✓ Tous les NomRole propagés")

    remaining_tel = df['NumeroTelephone'].isna()
    if remaining_tel.sum() > 0:
        print(f"  ⚠️  {remaining_tel.sum()} NumeroTelephone toujours manquants")
    else:
        print(f"  ✓ Tous les NumeroTelephone propagés")

    return df

def ajouter_code_role(df, col_nom='NomRole'):
    # Ajouter la colonne CodeRole à partir de NomRole.
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
    # Imputer NomRole à partir de ForfaitTND pour les forfaits explicitement connus.
    if 'NomRole' not in ChargesTelephoniques.columns or 'ForfaitTND' not in ChargesTelephoniques.columns:
        return ChargesTelephoniques
    print("[INFO] Imputation NomRole par ForfaitTND...")
    ChargesTelephoniques['NomRole'] = _normaliser_nom_role(ChargesTelephoniques['NomRole'])
    mask = ChargesTelephoniques['NomRole'].isna() | ChargesTelephoniques['NomRole'].eq('INCONNU')
    count_inconnu = mask.sum()
    print(f"  Lignes avec NomRole manquant ou 'INCONNU' : {count_inconnu}")
    
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



def _corriger_forfaits(ChargesTelephoniques):
    # Corriger les ForfaitTND par NomRole.
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



def transform_charges_telephoniques(ChargesTelephoniques):
    # Pipeline complet de transformation pour ChargesTelephoniques.
    ChargesTelephoniques = _corriger_types_telephoniques(ChargesTelephoniques)
    ChargesTelephoniques = _nettoyer_code_employee(ChargesTelephoniques)
    ChargesTelephoniques = normaliser_nom_departement_telephoniques(ChargesTelephoniques)
    ChargesTelephoniques = _propager_role_tel_dernier_mois(ChargesTelephoniques, 'DateOperation')
    ChargesTelephoniques = _corriger_forfaits(ChargesTelephoniques)
    ChargesTelephoniques = _imputer_nomrole_par_forfait(ChargesTelephoniques)
    ChargesTelephoniques = ajouter_code_departement(ChargesTelephoniques, 'NomDepartement')
    ChargesTelephoniques = ajouter_code_role(ChargesTelephoniques, 'NomRole')
    cols_doublon = [col for col in ChargesTelephoniques.columns if col != 'TelephoniqueID']
    ChargesTelephoniques = _supprimer_doublons(ChargesTelephoniques, cols_doublon, 'ChargesTelephoniques')
    ChargesTelephoniques = _forcer_colonnes_string(ChargesTelephoniques, ['CodeDepartement', 'CodeRole', 'CodeEmployee'])
    return ChargesTelephoniques

# ------------------------------------
# TRANSFORMATION CHARGES IMPRESSION

def _corriger_types_impression(ChargesImpression):
    # Corriger les types de données de ChargesImpression.
    ChargesImpression = _valider_et_convertir_dates(ChargesImpression, 'DateImpression', 'ChargesImpression')
    cols_string = ['NomDepartement', 'CouleurImpression', 'TypeImpression', 'FormatPapier']
    cols_exist = [col for col in cols_string if col in ChargesImpression.columns]
    for col in cols_exist:
        ChargesImpression[col] = ChargesImpression[col].astype('string')
    if 'CoutUnitaire' in ChargesImpression.columns:
        ChargesImpression['CoutUnitaire'] = ChargesImpression['CoutUnitaire'].fillna(0.0).astype('float64')
    return ChargesImpression

def normaliser_nom_departement_impression(df):
    # Normaliser NomDepartement pour ChargesImpression sans fallback.
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


def _valider_type_impression(ChargesImpression):
    # Valider TypeImpression : A3-COULEUR, A4-COULEUR, A3-NB, A4-NB, sinon INCONNU.
    if 'TypeImpression' not in ChargesImpression.columns:
        return ChargesImpression
    
    types_valides = ["A3-COULEUR", "A4-COULEUR", "A3-NB", "A4-NB"]
    print("[INFO] Validation TypeImpression...")
    
    ChargesImpression['TypeImpression'] = ChargesImpression['TypeImpression'].str.strip().str.upper()
    
    # Identifier les TypeImpression invalides
    invalid_mask = ~ChargesImpression['TypeImpression'].isin(types_valides)
    invalid_count = invalid_mask.sum()
    
    if invalid_count > 0:
        # Essayer de remplir avec CoutUnitaire
        if 'CoutUnitaire' in ChargesImpression.columns:
            mapping_cout_type = {
                0.026: "A4-NB",
                0.156: "A4-COULEUR",
                0.052: "A3-NB",
                0.313: "A3-COULEUR"
            }
            mapped_types = ChargesImpression.loc[invalid_mask, 'CoutUnitaire'].map(mapping_cout_type)
            valid_mapped_index = mapped_types.dropna().index
            
            if len(valid_mapped_index) > 0:
                ChargesImpression.loc[valid_mapped_index, 'TypeImpression'] = mapped_types.loc[valid_mapped_index]
                print(f"  ✓ {len(valid_mapped_index)} TypeImpression imputés par CoutUnitaire")
        
        # Marquer les restants comme INCONNU
        remaining_invalid = ~ChargesImpression['TypeImpression'].isin(types_valides)
        remaining_count = remaining_invalid.sum()
        if remaining_count > 0:
            ChargesImpression.loc[remaining_invalid, 'TypeImpression'] = 'INCONNU'
            print(f"  ⚠️  {remaining_count} TypeImpression resté(s) invalide(s) → 'INCONNU'")
    else:
        print(f"  ✓ Tous les types impression valides")
    
    return ChargesImpression


def _extraire_couleur_et_format(ChargesImpression):
    # Extraire CouleurImpression et FormatPapier à partir de TypeImpression.
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
    # Corriger CoutUnitaire selon TypeImpression.
    if 'TypeImpression' not in ChargesImpression.columns or 'CoutUnitaire' not in ChargesImpression.columns:
        return ChargesImpression
    
    mapping_format_cout = {
        "A4-NB": 0.026, "A3-COULEUR": 0.313, "A3-NB": 0.052, "A4-COULEUR": 0.156
    }
    
    print("[INFO] Correction CoutUnitaire...")
    type_norm = ChargesImpression['TypeImpression'].str.upper().str.strip()
    
    # 1. Corriger CoutUnitaire selon TypeImpression valide
    for format_type, cout in mapping_format_cout.items():
        ChargesImpression.loc[type_norm == format_type, 'CoutUnitaire'] = cout
    
    # 2. Marquer les CoutUnitaire qui ne correspondent pas aux valeurs valides → 0.0
    valid_costs = set(mapping_format_cout.values())  # {0.026, 0.156, 0.052, 0.313}
    invalid_mask = ~ChargesImpression['CoutUnitaire'].isin(valid_costs)
    invalid_count = invalid_mask.sum()
    
    if invalid_count > 0:
        ChargesImpression.loc[invalid_mask, 'CoutUnitaire'] = 0.0
        print(f"  ⚠️  {invalid_count} CoutUnitaire invalide(s) → 0.0")
    
    print(f"  ✓ CoutUnitaire corrigés")
    return ChargesImpression


def _corriger_nb_pages(ChargesImpression):
    # Rendre positives les valeurs de NbPages et remplir les vides avec 0.
    if 'NbPages' not in ChargesImpression.columns:
        return ChargesImpression
    print("[INFO] Correction NbPages...")
    ChargesImpression = ChargesImpression.dropna(subset=['NbPages'])
    ChargesImpression['NbPages'] = ChargesImpression['NbPages'].abs()
    ChargesImpression['NbPages'] = ChargesImpression['NbPages'].astype('int')
    print(f"  ✓ NbPages corrigés")
    return ChargesImpression



def transform_charges_impression(ChargesImpression):
    # Pipeline complet de transformation pour ChargesImpression.
    ChargesImpression = _corriger_types_impression(ChargesImpression)
    ChargesImpression = normaliser_nom_departement_impression(ChargesImpression)
    ChargesImpression = ajouter_code_departement(ChargesImpression, 'NomDepartement')
    ChargesImpression = _valider_type_impression(ChargesImpression)
    ChargesImpression = _extraire_couleur_et_format(ChargesImpression)
    ChargesImpression = _corriger_cout_unitaire(ChargesImpression)
    ChargesImpression = _corriger_nb_pages(ChargesImpression)
    ChargesImpression = _forcer_colonnes_string(ChargesImpression, ['CodeDepartement', 'FormatPapier', 'CouleurImpression'])
    return ChargesImpression
