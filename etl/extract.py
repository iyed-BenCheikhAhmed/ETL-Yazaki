import pandas as pd
from etl.config import get_source_connection


def extract_charges_telephoniques():
    # Extraire les données ChargesTelephoniques depuis SQL Server.
    conn = get_source_connection()
    try:
        ChargesTelephoniques = pd.read_sql("SELECT * FROM ChargesTelephoniques", conn)
    finally:
        conn.close()
    return ChargesTelephoniques


def extract_charges_impression():
    # Extraire les données ChargesImpression depuis SQL Server.
    conn = get_source_connection()
    try:
        ChargesImpression = pd.read_sql("SELECT * FROM ChargesImpression", conn)
    finally:
        conn.close()
    return ChargesImpression
