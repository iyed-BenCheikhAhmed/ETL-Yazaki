import pandas as pd
from etl.config import get_source_engine


def extract_charges_telephoniques():
    # Extraire les données ChargesTelephoniques depuis SQL Server.
    engine = get_source_engine()
    try:
        with engine.connect() as conn:
            ChargesTelephoniques = pd.read_sql("SELECT * FROM ChargesTelephoniques", conn)
        return ChargesTelephoniques
    except Exception as e:
        print(f"[ERROR] Extraction ChargesTelephoniques failed: {str(e)}")
        raise


def extract_charges_impression():
    # Extraire les données ChargesImpression depuis SQL Server.
    engine = get_source_engine()
    try:
        with engine.connect() as conn:
            ChargesImpression = pd.read_sql("SELECT * FROM ChargesImpression", conn)
        return ChargesImpression
    except Exception as e:
        print(f"[ERROR] Extraction ChargesImpression failed: {str(e)}")
        raise
