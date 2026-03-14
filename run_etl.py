"""
Script ETL standalone - appelé par Airflow DAG via BashOperator.
Utilise le Python Windows (Anaconda) qui a pyodbc et ODBC Driver 17.
"""
import sys
import os

# Use Windows-style path since this runs under Windows Python (Anaconda)
PROJECT_DIR = r"C:\Projet fin d'etude\ETL-Yazaki"
sys.path.insert(0, PROJECT_DIR)

from etl.extract import extract_charges_telephoniques, extract_charges_impression
from etl.transform import transform_charges_telephoniques, transform_charges_impression
from etl.load import load_all

def main():
    print("[ETL] Démarrage du pipeline ETL Yazaki...")

    print("[ETL] Extraction des données...")
    ChargesTelephoniques = extract_charges_telephoniques()
    ChargesImpression = extract_charges_impression()
    print(f"[ETL] ChargesTelephoniques: {ChargesTelephoniques.shape}")
    print(f"[ETL] ChargesImpression: {ChargesImpression.shape}")

    print("[ETL] Transformation des données...")
    ChargesTelephoniques = transform_charges_telephoniques(ChargesTelephoniques)
    ChargesImpression = transform_charges_impression(ChargesImpression)

    print("[ETL] Chargement dans le Data Warehouse...")
    load_all(ChargesTelephoniques, ChargesImpression)

    print("[ETL] Pipeline terminé avec succès!")

if __name__ == "__main__":
    main()
