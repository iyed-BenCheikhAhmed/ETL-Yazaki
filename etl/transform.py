import pandas as pd



def _imputer_par_mapping(df, col_cible, col_source, exclude_values=None):
    # Imputer les valeurs manquantes de col_cible en se basant sur col_source.
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


def normaliser_nom_departement(df):
    """ Normaliser les noms de departements (strip + casse correcte).
    Commun aux deux tables : ChargesTelephoniques et ChargesImpression.
    """
    df['NomDepartement'] = df['NomDepartement'].str.strip()

    # Remplacements en respectant l'ordre du notebook
    # (les plus longs d'abord pour eviter les collisions partielles)
    replacements_ordered = [
        ('production a', 'Production A'),
        ('production b', 'Production B'),
        ('logistique', 'Logistique'),
        ('engineering', 'Engineering'),
        ('finance', 'Finance'),
        ('qualite', 'Qualite'),
        ('direction', 'Direction'),
        ('cosee', 'Cosee'),
        ('achat', 'Achat'),
        ('plpp', 'PLPP'),
        ('ehs', 'EHS'),
        ('nys', 'NYS'),
        ('ols', 'OLS'),
        ('td', 'TD'),
        ('rh', 'RH'),
        ('it', 'IT'),
    ]
    for old, new in replacements_ordered:
        df['NomDepartement'] = df['NomDepartement'].str.replace(
            rf'\b{old}\b', new, case=False, regex=True
        )

    return df


# ---------------------------------------------------------------------------
# Transformation ChargesTelephoniques
# ---------------------------------------------------------------------------

def _corriger_types_telephoniques(ChargesTelephoniques):
    # Corriger les types de données de ChargesTelephoniques.
    
    ChargesTelephoniques['DateOperation'] = pd.to_datetime(ChargesTelephoniques['DateOperation'], format='mixed')
    for col in ['NomDepartement', 'CodeDepartement', 'NomResponsable',
                'NomRole', 'CodeEmployee', 'NumeroTel']:
        ChargesTelephoniques[col] = ChargesTelephoniques[col].astype('string')
    return ChargesTelephoniques


def _imputer_valeurs_telephoniques(ChargesTelephoniques):
    # Imputer toutes les valeurs manquantes de ChargesTelephoniques.

    # CodeDepartement <- NomDepartement
    ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'CodeDepartement', 'NomDepartement')
    # NomDepartement <- CodeDepartement
    ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'NomDepartement', 'CodeDepartement')
    # NomResponsable <- NomDepartement
    ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'NomResponsable', 'NomDepartement')
    # NomRole <- CodeEmployee
    ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'NomRole', 'CodeEmployee')
    # ForfaitTND <- NomRole
    ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'ForfaitTND', 'NomRole')
    return ChargesTelephoniques


def _nettoyer_responsable_inconnu(ChargesTelephoniques):
    # Remplacer les NomResponsable 'Inconnu' par le vrai responsable du departement.

    ChargesTelephoniques = _imputer_par_mapping(ChargesTelephoniques, 'NomResponsable', 'NomDepartement',
                              exclude_values=['Inconnu'])
    return ChargesTelephoniques


def _nettoyer_code_employee(ChargesTelephoniques):
    # Mettre en majuscule et strip les CodeEmployee.
    
    ChargesTelephoniques['CodeEmployee'] = ChargesTelephoniques['CodeEmployee'].str.upper().str.strip()
    return ChargesTelephoniques


def _corriger_forfaits(ChargesTelephoniques):
    # Corriger les ForfaitTND par NomRole.
    corrections_forfait = {
        'Line Leader': 20,
        'Technicien': 25,
        'Assistante DG': 80,
        'Superviseur': 50,
        'Head': 0,
    }
    for role, forfait in corrections_forfait.items():
        ChargesTelephoniques.loc[ChargesTelephoniques['NomRole'] == role, 'ForfaitTND'] = forfait
    return ChargesTelephoniques


def _ajouter_code_role(ChargesTelephoniques):
    # Ajouter la colonne CodeRole et la placer juste avant NomRole.
    code_role_mapping = {
        'Manager': 'MGR',
        'Specialiste': 'SP',
        'Technicien': 'TECH',
        'Superviseur': 'SUP',
        'Line Leader': 'LL',
        'Head': 'HD',
        'Assistante DG': 'ASS-DG',
        'Central Function Manager': 'CFM',
        'Central Function': 'CF',
        'Team Manager': 'TM',
    }
    ChargesTelephoniques['CodeRole'] = ChargesTelephoniques['NomRole'].map(code_role_mapping).astype('string')

    # Placer CodeRole juste avant NomRole
    cols = list(ChargesTelephoniques.columns)
    cols.remove('CodeRole')
    idx = cols.index('NomRole')
    cols.insert(idx, 'CodeRole')
    ChargesTelephoniques = ChargesTelephoniques[cols]
    return ChargesTelephoniques


def transform_charges_telephoniques(ChargesTelephoniques):
    # Pipeline complet de transformation pour ChargesTelephoniques.

    ChargesTelephoniques = _corriger_types_telephoniques(ChargesTelephoniques)
    ChargesTelephoniques = _imputer_valeurs_telephoniques(ChargesTelephoniques)
    ChargesTelephoniques = normaliser_nom_departement(ChargesTelephoniques)
    ChargesTelephoniques = _nettoyer_responsable_inconnu(ChargesTelephoniques)
    ChargesTelephoniques = _nettoyer_code_employee(ChargesTelephoniques)
    ChargesTelephoniques = _corriger_forfaits(ChargesTelephoniques)
    ChargesTelephoniques = _ajouter_code_role(ChargesTelephoniques)
    return ChargesTelephoniques


# ---------------------------------------------------------------------------
# Transformation ChargesImpression
# ---------------------------------------------------------------------------

def _corriger_types_impression(ChargesImpression):
    # Corriger les types de données de ChargesImpression.

    ChargesImpression['DateImpression'] = pd.to_datetime(ChargesImpression['DateImpression'], format='mixed')
    for col in ['NomDepartement', 'CodeDepartement', 'NomResponsable',
                'CouleurImpression', 'CodeDetailImpression', 'Format']:
        ChargesImpression[col] = ChargesImpression[col].astype('string')
    return ChargesImpression


def _imputer_valeurs_impression(ChargesImpression):
    # Imputer toutes les valeurs manquantes de ChargesImpression.
    
    # NomDepartement <- CodeDepartement
    ChargesImpression = _imputer_par_mapping(ChargesImpression, 'NomDepartement', 'CodeDepartement')
    # CodeDepartement <- NomDepartement
    ChargesImpression = _imputer_par_mapping(ChargesImpression, 'CodeDepartement', 'NomDepartement')
    # NomResponsable <- NomDepartement (exclure 'Inconnu')
    ChargesImpression['NomResponsable'] = ChargesImpression['NomResponsable'].str.strip()
    ChargesImpression = _imputer_par_mapping(ChargesImpression, 'NomResponsable', 'NomDepartement',
                              exclude_values=['Inconnu'])
    # CoutUnitaire <- CodeDetailImpression
    ChargesImpression = _imputer_par_mapping(ChargesImpression, 'CoutUnitaire', 'CodeDetailImpression')
    return ChargesImpression


def _nettoyer_couleur_impression(ChargesImpression):
    # Garder uniquement la couleur (supprimer le format A4/A3).
    ChargesImpression['CouleurImpression'] = ChargesImpression['CouleurImpression'].str.split(' - ').str[1].astype('string')
    return ChargesImpression


def _corriger_nb_pages(ChargesImpression):
    # Rendre positives les valeurs négatives de NbPages.
    ChargesImpression['NbPages'] = ChargesImpression['NbPages'].abs()
    return ChargesImpression


def transform_charges_impression(ChargesImpression):
    # Pipeline complet de transformation pour ChargesImpression.
    ChargesImpression = _corriger_types_impression(ChargesImpression)
    ChargesImpression = normaliser_nom_departement(ChargesImpression)
    ChargesImpression = _imputer_valeurs_impression(ChargesImpression)
    ChargesImpression = _nettoyer_couleur_impression(ChargesImpression)
    ChargesImpression = _corriger_nb_pages(ChargesImpression)
    return ChargesImpression
