# Airflow + Docker pour ETL Yazaki

## Prerequis
- Docker Desktop installe et demarre.
- SQL Server accessible depuis Docker via host.docker.internal\\SQLEXPRESS.

## Demarrage
1. Ouvrir un terminal dans ETL-Yazaki/airflow.
2. Lancer:
   docker compose up airflow-init
3. Puis:
   docker compose up -d
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
