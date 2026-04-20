import pandas as pd
import numpy as np
from sqlalchemy import text
from etl.config import get_dw_engine


def _clear_tables(engine):
    """Vider les tables de faits puis les dimensions (y compris Dim_Temps)."""
    print("[LOAD] Suppression des données existantes...")
    with engine.begin() as conn:
        # Supprimer d'abord les prévisions (FK vers Dim_Departement)
        conn.execute(text("DELETE FROM Previsions"))
        # Supprimer les faits d'abord (clés étrangères)
        conn.execute(text("DELETE FROM Fact_Telephone"))
        conn.execute(text("DELETE FROM Fact_Impression"))
        # Puis Dim_Temps (plus de FK après suppression des faits)
        conn.execute(text("DELETE FROM Dim_Temps"))
        # Puis les dimensions
        conn.execute(text("DELETE FROM Dim_Impression"))
        conn.execute(text("DELETE FROM Dim_Role"))
        conn.execute(text("DELETE FROM Dim_Employee"))
        conn.execute(text("DELETE FROM Dim_Departement"))
        # Reinitialiser les compteurs IDENTITY
        for table in ['Previsions', 'Fact_Telephone', 'Fact_Impression', 'Dim_Impression', 
                      'Dim_Role', 'Dim_Employee', 'Dim_Departement']:
            try:
                conn.execute(text(f"DBCC CHECKIDENT ('{table}', RESEED, 0)"))
            except:
                pass
    print("  ✓ Tables vidées (Dim_Temps incluse)")


def _read_dim(engine, table_name):
    """Lire une table de dimension depuis le DW."""
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)


def load_dim_departement(engine, df_charges_tel, df_charges_imp):
    """Charger les départements uniques dans Dim_Departement."""
    print("[LOAD] Chargement Dim_Departement...")
    
    # Extraire les départements des deux DataFrames
    depts = set(df_charges_tel['NomDepartement'].dropna().unique()) | set(df_charges_imp['NomDepartement'].dropna().unique())
    
    df_depts = pd.DataFrame({
        'NomDepartement': sorted(list(depts)),
        'CodeDepartement': None,
        'DescriptionDepartement': None
    })

    if 'CodeDepartement' in df_charges_tel.columns or 'CodeDepartement' in df_charges_imp.columns:
        codes = {}
        if 'NomDepartement' in df_charges_tel.columns and 'CodeDepartement' in df_charges_tel.columns:
            codes.update(
                df_charges_tel[['NomDepartement', 'CodeDepartement']]
                .dropna()
                .drop_duplicates(subset=['NomDepartement'])
                .set_index('NomDepartement')['CodeDepartement']
                .to_dict()
            )
        if 'NomDepartement' in df_charges_imp.columns and 'CodeDepartement' in df_charges_imp.columns:
            codes.update(
                df_charges_imp[['NomDepartement', 'CodeDepartement']]
                .dropna()
                .drop_duplicates(subset=['NomDepartement'])
                .set_index('NomDepartement')['CodeDepartement']
                .to_dict()
            )
        df_depts['CodeDepartement'] = df_depts['NomDepartement'].map(codes).fillna('INCONNU')
    else:
        df_depts['CodeDepartement'] = 'INCONNU'
    
    with engine.begin() as conn:
        for _, row in df_depts.iterrows():
            conn.execute(text("""
                INSERT INTO Dim_Departement (CodeDepartement, NomDepartement, DescriptionDepartement)
                VALUES (:code, :nom, :desc)
            """), {
                'code': row['CodeDepartement'],
                'nom': row['NomDepartement'],
                'desc': row['DescriptionDepartement']
            })
    
    print(f"  ✓ {len(df_depts)} département(s) chargé(s)")


def load_dim_role(engine, df_charges_tel):
    """Charger les rôles uniques dans Dim_Role."""
    print("[LOAD] Chargement Dim_Role...")
    
    roles = df_charges_tel['NomRole'].dropna().drop_duplicates().reset_index(drop=True)
    df_roles = pd.DataFrame({
        'NomRole': roles,
        'CodeRole': None,
        'DescriptionRole': None
    })

    if 'CodeRole' in df_charges_tel.columns:
        role_code_map = (
            df_charges_tel[['NomRole', 'CodeRole']]
            .dropna()
            .drop_duplicates(subset=['NomRole'])
            .set_index('NomRole')['CodeRole']
            .to_dict()
        )
        df_roles['CodeRole'] = df_roles['NomRole'].map(role_code_map).fillna('INCONNU')
    else:
        df_roles['CodeRole'] = 'INCONNU'
    
    with engine.begin() as conn:
        for _, row in df_roles.iterrows():
            conn.execute(text("""
                INSERT INTO Dim_Role (CodeRole, NomRole, DescriptionRole)
                VALUES (:code, :nom, :desc)
            """), {
                'code': row['CodeRole'],
                'nom': row['NomRole'],
                'desc': row['DescriptionRole']
            })
    
    print(f"  ✓ {len(df_roles)} rôle(s) chargé(s)")


def load_dim_employee(engine, df_charges_tel):
    """Charger les employés uniques dans Dim_Employee."""
    print("[LOAD] Chargement Dim_Employee...")
    
    employees = df_charges_tel[['CodeEmployee', 'NumeroTelephone']].drop_duplicates().reset_index(drop=True)
    
    with engine.begin() as conn:
        for _, row in employees.iterrows():
            conn.execute(text("""
                INSERT INTO Dim_Employee (CodeEmployee, NumeroTelephone)
                VALUES (:code, :tel)
            """), {'code': row['CodeEmployee'], 'tel': row['NumeroTelephone']})
    
    print(f"  ✓ {len(employees)} employé(s) chargé(s)")


def load_dim_temps(engine, df_charges_tel, df_charges_imp):
    """(Re)charger Dim_Temps avec des DateID séquentiels (1..n) triés par date."""
    print("[LOAD] (Re)chargement Dim_Temps (DateID = 1..n)...")

    # Extraire les dates des deux DataFrames (en ignorant les NaT)
    dates = []
    if 'DateOperation' in df_charges_tel.columns:
        dates_tel = pd.to_datetime(df_charges_tel['DateOperation'], errors='coerce').dropna().dt.date
        dates.extend(dates_tel.tolist())
    if 'DateImpression' in df_charges_imp.columns:
        dates_imp = pd.to_datetime(df_charges_imp['DateImpression'], errors='coerce').dropna().dt.date
        dates.extend(dates_imp.tolist())

    if not dates:
        raise ValueError("Aucune date valide trouvée pour alimenter Dim_Temps")

    date_min = min(dates)
    date_max = max(dates)

    # Générer toutes les dates de l'intervalle (incluant les trous)
    all_dates = pd.date_range(start=date_min, end=date_max, freq='D')

    # Mappings FR simples (évite dépendance locale OS)
    mois_fr = {
        1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril", 5: "Mai", 6: "Juin",
        7: "Juillet", 8: "Août", 9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre",
    }
    jour_fr = {
        0: "Lundi", 1: "Mardi", 2: "Mercredi", 3: "Jeudi", 4: "Vendredi", 5: "Samedi", 6: "Dimanche",
    }

    df_dim = pd.DataFrame({
        'DateComplete': all_dates.date,
        'Annee': all_dates.year,
        'Mois': all_dates.month,
        'Jour': all_dates.day,
        'Trimestre': ((all_dates.month - 1) // 3 + 1).astype(int),
        'NomMois': [mois_fr.get(m, str(m)) for m in all_dates.month],
        'NomJour': [jour_fr.get(dow, "") for dow in all_dates.dayofweek],
    })
    df_dim.insert(0, 'DateID', range(1, len(df_dim) + 1))

    with engine.begin() as conn:
        # La table a été vidée dans _clear_tables; on insère tout en une fois
        conn.execute(
            text("""
                INSERT INTO Dim_Temps (DateID, DateComplete, Annee, Mois, NomMois, Trimestre, Jour, NomJour)
                VALUES (:DateID, :DateComplete, :Annee, :Mois, :NomMois, :Trimestre, :Jour, :NomJour)
            """),
            df_dim.to_dict(orient='records')
        )

    print(f"  ✓ Dim_Temps chargée: {len(df_dim)} date(s) ({date_min} → {date_max})")


def load_dim_impression(engine, df_charges_imp):
    """Charger les types d'impression uniques dans Dim_Impression."""
    print("[LOAD] Chargement Dim_Impression...")
    
    impressions = df_charges_imp[['TypeImpression', 'CouleurImpression', 'FormatPapier']].drop_duplicates().reset_index(drop=True)
    
    with engine.begin() as conn:
        for _, row in impressions.iterrows():
            conn.execute(text("""
                INSERT INTO Dim_Impression (TypeImpression, CouleurImpression, FormatPapier)
                VALUES (:type, :couleur, :format)
            """), {
                'type': row['TypeImpression'],
                'couleur': row['CouleurImpression'],
                'format': row['FormatPapier']
            })
    
    print(f"  ✓ {len(impressions)} type(s) d'impression chargé(s)")


def load_fact_telephone(engine, df_charges_tel):
    """Charger les faits téléphoniques avec résolution des clés étrangères."""
    print("[LOAD] Chargement Fact_Telephone...")
    
    # Lire les dimensions
    dim_dept = _read_dim(engine, 'Dim_Departement')
    dim_emp = _read_dim(engine, 'Dim_Employee')
    dim_role = _read_dim(engine, 'Dim_Role')
    dim_temps = _read_dim(engine, 'Dim_Temps')
    
    # Créer les mappings
    dept_map = dict(zip(dim_dept['NomDepartement'], dim_dept['DepartementID']))
    emp_map = dict(zip(dim_emp['CodeEmployee'], dim_emp['EmployeeID']))
    role_map = dict(zip(dim_role['NomRole'], dim_role['RoleID']))
    temps_map = dict(zip(
        pd.to_datetime(dim_temps['DateComplete']).dt.date,
        dim_temps['DateID']
    ))
    
    # Préparer les données
    fact = df_charges_tel.copy()
    fact['DepartementID'] = fact['NomDepartement'].map(dept_map)
    fact['EmployeeID'] = fact['CodeEmployee'].map(emp_map)
    fact['RoleID'] = fact['NomRole'].map(role_map)
    fact['DateID'] = pd.to_datetime(fact['DateOperation']).dt.date.map(temps_map)
    
    # Extraire les colonnes nécessaires (exclure les lignes avec FK manquantes)
    fact = fact[['DepartementID', 'EmployeeID', 'RoleID', 'DateID', 'ForfaitTND']].dropna()
    
    count = 0
    with engine.begin() as conn:
        for _, row in fact.iterrows():
            conn.execute(text("""
                INSERT INTO Fact_Telephone (DepartementID, EmployeeID, RoleID, DateID, ForfaitTND)
                VALUES (:dept_id, :emp_id, :role_id, :date_id, :forfait)
            """), {
                'dept_id': int(row['DepartementID']),
                'emp_id': int(row['EmployeeID']),
                'role_id': int(row['RoleID']),
                'date_id': int(row['DateID']),
                'forfait': float(row['ForfaitTND'])
            })
            count += 1
    
    print(f"  ✓ {count} ligne(s) chargée(s) dans Fact_Telephone")


def load_fact_impression(engine, df_charges_imp):
    """Charger les faits impression avec résolution des clés étrangères."""
    print("[LOAD] Chargement Fact_Impression...")
    
    # Lire les dimensions
    dim_dept = _read_dim(engine, 'Dim_Departement')
    dim_imp = _read_dim(engine, 'Dim_Impression')
    dim_temps = _read_dim(engine, 'Dim_Temps')
    
    # Créer les mappings
    dept_map = dict(zip(dim_dept['NomDepartement'], dim_dept['DepartementID']))
    temps_map = dict(zip(
        pd.to_datetime(dim_temps['DateComplete']).dt.date,
        dim_temps['DateID']
    ))
    
    # Créer un mapping pour les impressions (par combinaison de colonnes)
    imp_tuple_to_id = {}
    for _, row in dim_imp.iterrows():
        key = (row['TypeImpression'], row['CouleurImpression'], row['FormatPapier'])
        imp_tuple_to_id[key] = row['ImpressionID']
    
    # Préparer les données
    fact = df_charges_imp.copy()
    fact['DepartementID'] = fact['NomDepartement'].map(dept_map)
    fact['DateID'] = pd.to_datetime(fact['DateImpression']).dt.date.map(temps_map)
    fact['ImpressionID'] = fact.apply(
        lambda row: imp_tuple_to_id.get((row['TypeImpression'], row['CouleurImpression'], row['FormatPapier'])),
        axis=1
    )
    
    # Extraire les colonnes nécessaires (exclure les lignes avec FK manquantes)
    fact = fact[['DepartementID', 'DateID', 'ImpressionID', 'NbPages', 'CoutUnitaire']].dropna()
    
    count = 0
    with engine.begin() as conn:
        for _, row in fact.iterrows():
            conn.execute(text("""
                INSERT INTO Fact_Impression (DepartementID, DateID, ImpressionID, NbPages, CoutUnitaire)
                VALUES (:dept_id, :date_id, :imp_id, :nb_pages, :cout)
            """), {
                'dept_id': int(row['DepartementID']),
                'date_id': int(row['DateID']),
                'imp_id': int(row['ImpressionID']),
                'nb_pages': int(row['NbPages']),
                'cout': float(row['CoutUnitaire'])
            })
            count += 1
    
    print(f"  ✓ {count} ligne(s) chargée(s) dans Fact_Impression")


def load_all(df_charges_tel, df_charges_imp):
    """Pipeline complet de chargement."""
    print("\n" + "="*80)
    print("[PIPELINE] LOAD - Chargement des données dans DW_Yazaki")
    print("="*80 + "\n")
    
    engine = get_dw_engine()
    
    try:
        # Vider les données existantes (sauf Dim_Temps)
        _clear_tables(engine)
        
        # Charger les dimensions
        load_dim_departement(engine, df_charges_tel, df_charges_imp)
        load_dim_role(engine, df_charges_tel)
        load_dim_employee(engine, df_charges_tel)
        load_dim_temps(engine, df_charges_tel, df_charges_imp)
        load_dim_impression(engine, df_charges_imp)
        
        # Charger les faits
        load_fact_telephone(engine, df_charges_tel)
        load_fact_impression(engine, df_charges_imp)
        
        print("\n" + "="*80)
        print("[SUCCÈS] Chargement terminé avec succès dans DW_Yazaki !")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n[ERREUR] Erreur lors du chargement : {str(e)}\n")
        raise
    finally:
        engine.dispose()
