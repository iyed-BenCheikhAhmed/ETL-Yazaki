import pandas as pd
from sqlalchemy import text
from etl.config import get_dw_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clear_tables(engine):
    """Vider les tables de faits puis les dimensions ."""
    with engine.begin() as conn:
        # Faits d'abord (FK vers dimensions)
        conn.execute(text("DELETE FROM Fact_Telephone"))
        conn.execute(text("DELETE FROM Fact_Impression"))
        # Dimensions 
        conn.execute(text("DELETE FROM Dim_Departement"))
        conn.execute(text("DELETE FROM Dim_Employee"))
        conn.execute(text("DELETE FROM Dim_Role"))
        conn.execute(text("DELETE FROM Dim_Impression"))
        # Reinitialiser les compteurs IDENTITY pour que les IDs commencent a 1
        for table in ['Dim_Departement', 'Dim_Employee', 'Dim_Role',
                       'Dim_Impression', 'Fact_Telephone', 'Fact_Impression']:
            last_val = conn.execute(text(
                f"SELECT CAST(last_value AS BIGINT) FROM sys.identity_columns "
                f"WHERE object_id = OBJECT_ID('{table}')"
            )).scalar()
            if last_val is not None:
                conn.execute(text(f"DBCC CHECKIDENT ('{table}', RESEED, 0)"))
    print("[LOAD] Tables videes (Dim_Temps conservee)")


def _read_dim(engine, table_name):
    """Lire une table de dimension depuis le DW."""
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)


# ---------------------------------------------------------------------------
# Chargement des dimensions
# ---------------------------------------------------------------------------

def load_dim_departement(engine, ct, ci):
    """Charger Dim_Departement depuis les deux DataFrames transformes."""
    deps_tel = ct[['CodeDepartement', 'NomDepartement', 'NomResponsable']].drop_duplicates()
    deps_imp = ci[['CodeDepartement', 'NomDepartement', 'NomResponsable']].drop_duplicates()
    deps = (
        pd.concat([deps_tel, deps_imp])
        .drop_duplicates(subset='CodeDepartement')
        .reset_index(drop=True)
    )

    description_dept_map = {
        'Production A': 'Ligne de production P1 et P2',
        'Production B': 'Ligne de production P3',
        'Finance': 'Gestion financière et comptabilité',
        'IT': 'Support informatique et systèmes',
        'RH': 'Gestion des ressources humaines',
        'Logistique': 'Gestion des stocks et transport',
        'TD': 'Maintenance industrielle',
        'Direction': 'Administration générale',
        'PLPP': 'Pilotage des ordres de production',
        'Qualite': 'Contrôle qualité',
        'Achat': 'Gestion des achats fournisseurs',
        'NYS': 'Gestion des affichages industriels',
        'EHS': 'Hygiène, Sécurité et Environnement',
        'OLS': 'Recrutement et formation',
        'Cosee': 'Maintenance des tables de production',
        'Engineering': 'Conception, développement et optimisation des systèmes industriels',
    }

    deps['DescriptionDept'] = deps['NomDepartement'].map(description_dept_map)
    deps = deps[['CodeDepartement', 'NomDepartement', 'DescriptionDept', 'NomResponsable']]
    deps.to_sql('Dim_Departement', engine, if_exists='append', index=False)
    print(f"[LOAD] {len(deps)} lignes chargees dans Dim_Departement")


def load_dim_employee(engine, ct):
    """Charger Dim_Employee depuis ChargesTelephoniques transforme."""
    emps = (
        ct[['CodeEmployee', 'NumeroTel']]
        .drop_duplicates(subset='CodeEmployee')
        .reset_index(drop=True)
    )
    # Rename NumeroTel to NumeroEmployee to match the database schema
    emps = emps[['CodeEmployee', 'NumeroTel']].rename(columns={'NumeroTel': 'NumeroEmployee'})
    emps.to_sql('Dim_Employee', engine, if_exists='append', index=False)
    print(f"[LOAD] {len(emps)} lignes chargees dans Dim_Employee")


def load_dim_role(engine, ct):
    """Charger Dim_Role depuis ChargesTelephoniques transforme."""
    roles = (
        ct[['CodeRole', 'NomRole']]
        .drop_duplicates(subset='CodeRole')
        .reset_index(drop=True)
    )

    niveau_map = {
        'Head': 1,
        'Assistante DG': 2,
        'Manager': 3,
        'Central Function Manager': 4,
        'Central Function': 5,
        'Team Manager': 6,
        'Superviseur Comite Direction': 7,
        'Superviseur': 8,
        'Specialiste': 9,
        'Technicien': 10,
        'Line Leader': 11,
    }

    description_map = {
        'Head': 'Responsable stratégique, chargé de la prise de décision, de la planification et du pilotage global des activités.',
        'Assistante DG': 'Assistante de la Direction Générale, en charge du support administratif, de l\'organisation et de la coordination des activités exécutives.',
        'Manager': 'Responsable d\'équipe ou de service, chargé de la gestion opérationnelle et du suivi des performances.',
        'Central Function Manager': 'Responsable d\'une fonction centrale (RH, Finance, IT, etc.), supervisant les opérations et les ressources associées.',
        'Central Function': 'Employé appartenant à une fonction support centrale (RH, Finance, IT, Achats, etc.).',
        'Team Manager': 'Manager direct d\'une équipe opérationnelle, responsable de la supervision quotidienne et de l\'atteinte des objectifs.',
        'Superviseur Comite Direction': 'Responsable de la coordination et du suivi des activités liées au comité de direction.',
        'Superviseur': 'Encadrant intermédiaire supervisant les activités quotidiennes d\'une équipe.',
        'Specialiste': 'Employé senior ou spécialiste technique, responsable d\'un groupe ou d\'un shift de production.',
        'Technicien': 'Employé technique ou administratif assurant l\'exécution des tâches opérationnelles.',
        'Line Leader': 'Responsable d\'une ligne de production, chargé du bon déroulement des opérations et du respect des standards.',
    }

    roles['NiveauHierarchique'] = roles['NomRole'].map(niveau_map)
    roles['DescriptionRole'] = roles['NomRole'].map(description_map)
    roles = roles[['CodeRole', 'NomRole', 'NiveauHierarchique', 'DescriptionRole']]
    roles.to_sql('Dim_Role', engine, if_exists='append', index=False)
    print(f"[LOAD] {len(roles)} lignes chargees dans Dim_Role")


def load_dim_impression(engine, ci):
    """Charger Dim_Impression depuis ChargesImpression transforme."""
    imps = (
        ci[['CodeDetailImpression', 'CouleurImpression', 'Format']]
        .drop_duplicates(subset='CodeDetailImpression')
        .reset_index(drop=True)
    )
    imps = imps.rename(columns={
        'CodeDetailImpression': 'CodeImpression',
        'Format': 'FormatPapier',
    })
    imps = imps[['CodeImpression', 'CouleurImpression', 'FormatPapier']]
    imps.to_sql('Dim_Impression', engine, if_exists='append', index=False)
    print(f"[LOAD] {len(imps)} lignes chargees dans Dim_Impression")


# ---------------------------------------------------------------------------
# Chargement des faits
# ---------------------------------------------------------------------------

def load_fact_telephone(engine, ct):
    """Charger Fact_Telephone avec les FK resolues."""
    dim_dept = _read_dim(engine, 'Dim_Departement')
    dim_emp = _read_dim(engine, 'Dim_Employee')
    dim_role = _read_dim(engine, 'Dim_Role')
    dim_temps = _read_dim(engine, 'Dim_Temps')

    fact = ct.copy()

    # Lookup DepartementID
    dept_map = dim_dept.set_index('CodeDepartement')['DepartementID'].to_dict()
    fact['DepartementID'] = fact['CodeDepartement'].map(dept_map)

    # Lookup EmployeeID
    emp_map = dim_emp.set_index('CodeEmployee')['EmployeeID'].to_dict()
    fact['EmployeeID'] = fact['CodeEmployee'].map(emp_map)

    # Lookup RoleID
    role_map = dim_role.set_index('CodeRole')['RoleID'].to_dict()
    fact['RoleID'] = fact['CodeRole'].map(role_map)

    # Lookup DateID
    if not dim_temps.empty:
        temps_map = dict(zip(
            pd.to_datetime(dim_temps['DateComplete']).dt.date,
            dim_temps['DateID']
        ))
        fact['DateID'] = fact['DateOperation'].dt.date.map(temps_map)
    else:
        fact['DateID'] = None

    fact = fact[['DepartementID', 'EmployeeID', 'RoleID', 'DateID',
                 'ForfaitTND', 'ChargesVariables']]
    fact.to_sql('Fact_Telephone', engine, if_exists='append', index=False)
    print(f"[LOAD] {len(fact)} lignes chargees dans Fact_Telephone")


def load_fact_impression(engine, ci):
    """Charger Fact_Impression avec les FK resolues."""
    dim_dept = _read_dim(engine, 'Dim_Departement')
    dim_imp = _read_dim(engine, 'Dim_Impression')
    dim_temps = _read_dim(engine, 'Dim_Temps')

    fact = ci.copy()

    # Lookup DepartementID
    dept_map = dim_dept.set_index('CodeDepartement')['DepartementID'].to_dict()
    fact['DepartementID'] = fact['CodeDepartement'].map(dept_map)

    # Lookup ImpressionID
    imp_map = dim_imp.set_index('CodeImpression')['ImpressionID'].to_dict()
    fact['ImpressionID'] = fact['CodeDetailImpression'].map(imp_map)

    # Lookup DateID
    if not dim_temps.empty:
        temps_map = dict(zip(
            pd.to_datetime(dim_temps['DateComplete']).dt.date,
            dim_temps['DateID']
        ))
        fact['DateID'] = fact['DateImpression'].dt.date.map(temps_map)
    else:
        fact['DateID'] = None

    fact = fact[['DepartementID', 'DateID', 'ImpressionID',
                 'NbPages', 'CoutUnitaire']]
    fact.to_sql('Fact_Impression', engine, if_exists='append', index=False)
    print(f"[LOAD] {len(fact)} lignes chargees dans Fact_Impression")


# ---------------------------------------------------------------------------
# Pipeline complet
# ---------------------------------------------------------------------------

def load_all(ct, ci):
    """Pipeline complet de chargement dans le Data Warehouse DW_Yazaki."""
    engine = get_dw_engine()

    # 1. Vider les tables (faits puis dimensions, sauf Dim_Temps)
    _clear_tables(engine)

    # 2. Charger les dimensions
    load_dim_departement(engine, ct, ci)
    load_dim_employee(engine, ct)
    load_dim_role(engine, ct)
    load_dim_impression(engine, ci)

    # 3. Charger les faits
    load_fact_telephone(engine, ct)
    load_fact_impression(engine, ci)

    print("[LOAD] Chargement termine avec succes")
