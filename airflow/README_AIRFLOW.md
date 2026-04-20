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
Le DAG cree est: etl_yazaki_dw_previsions

Il execute:
- extract depuis Yazaki_Source
- transform
- load dans DW_Yazaki
- generation + chargement des previsions dans DW_Yazaki.dbo.Previsions

## Refresh Power BI
- Mode On-Premises Data Gateway (recommande si votre dataset est deja configure dans Power BI Service):
   - Dans `airflow/.env`, garder `POWERBI_REFRESH_MODE=onprem_gateway`.
   - La tache `refresh_powerbi_dataset` ne fera pas d'appel API; le refresh est gere par la gateway/planification Power BI.
- Mode API Service Principal (optionnel):
   - Mettre `POWERBI_REFRESH_MODE=service_principal`.
   - Renseigner `POWERBI_TENANT_ID`, `POWERBI_CLIENT_ID`, `POWERBI_CLIENT_SECRET`, `POWERBI_WORKSPACE_ID`, `POWERBI_DATASET_ID`.

## Arret
- docker compose down
