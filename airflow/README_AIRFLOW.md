# Airflow + Docker pour ETL Yazaki

## Prerequis
- Docker Desktop installé et demarré.
- SQL Server accessible depuis Docker via host.docker.internal\\SQLEXPRESS.

## Demarrage
1. Ouvrir un terminal dans ETL-Yazaki/airflow.
2. Lancer:
## Initialise Airflow : crée la base de données 
## interne, l'utilisateur admin par défaut, etc.
   docker compose up airflow-init 
3. Puis:
   docker compose up -d 
   # -d detached pour reduire les logs en direct
4. Ouvrir Airflow: http://localhost:8080
   - login: admin
   - password: admin

## DAG
Le DAG cree est: etl_yazaki

Il execute:
- extract depuis Yazaki_Source
- transform
- load dans DW_Yazaki

## Arret
- docker compose down
