# Guide d'utilisation d'Airflow pour ETL-Yazaki

## ✅ Installation terminée avec succès!

Airflow 3.1.8 est installé dans WSL avec toutes les dépendances nécessaires.

## 📂 Structure

- **Environnement virtuel**: `~/airflow-env` (dans WSL)
- **Airflow Home**: `~/airflow` (dans WSL)
- **Projet**: `~/ETL-Yazaki` (dans WSL)
- **DAGs**: `~/ETL-Yazaki/dags` → lié à `~/airflow/dags`

## 🚀 Démarrer Airflow

### Option 1: Utiliser le script de démarrage

Dans votre terminal WSL:
```bash
cd ~/ETL-Yazaki
bash start_airflow.sh
```

### Option 2: Démarrer manuellement

```bash
export AIRFLOW_HOME=~/airflow
source ~/airflow-env/bin/activate
airflow standalone
```

## 🌐 Accéder à l'interface web

Une fois Airflow démarré:
1. Ouvrez votre navigateur
2. Allez à: http://localhost:8080
3. Utilisez les identifiants affichés dans les logs (username: `admin`)

## 📝 Utiliser votre DAG

Votre DAG `yazaki_etl_dag.py` sera automatiquement détecté par Airflow.

Pour que le DAG fonctionne, vous devez:
1. **Vérifier les chemins** dans le DAG pour qu'ils pointent vers les fichiers dans WSL
2. **Mettre à jour `config.py`** si nécessaire pour la connexion à SQL Server depuis WSL

### Connexion SQL Server depuis WSL

⚠️ **Important**: WSL utilise un réseau virtuel. Pour vous connecter à SQL Server qui tourne sur Windows:
- Utilisez l'adresse IP de Windows au lieu de `localhost`
- Dans PowerShell, trouvez votre IP: `ipconfig` (cherchez l'adapteur WSL)
- Ou utilisez: `host.docker.internal` ou l'IP de l'interface vEthernet

```python
# Dans config.py, remplacez:
SERVER = r'LAPTOP-65SMMFRO\MSSQLSERVER2'
# Par:
SERVER = r'<VOTRE_IP_WINDOWS>\MSSQLSERVER2'
```

## 🔧 Commandes utiles

### Activer l'environnement virtuel
```bash
source ~/airflow-env/bin/activate
```

### Tester un DAG
```bash
export AIRFLOW_HOME=~/airflow
airflow dags test yazaki_etl_dag 2026-03-12
```

### Voir les DAGs disponibles
```bash
airflow dags list
```

### Arrêter Airflow
Appuyez sur `Ctrl+C` dans le terminal où Airflow tourne

## 🛠️ Dépannage

### Le DAG n'apparaît pas
1. Vérifiez les erreurs: `airflow dags list-import-errors`
2. Vérifiez les logs: `ls ~/airflow/logs/`

### Erreur de connexion SQL Server
1. Vérifiez que SQL Server accepte les connexions TCP/IP
2. Vérifiez le pare-feu Windows
3. Utilisez l'IP Windows au lieu de localhost

### Réinitialiser Airflow
```bash
rm -rf ~/airflow
export AIRFLOW_HOME=~/airflow
airflow db migrate
cd ~/ETL-Yazaki && ln -sf ~/ETL-Yazaki/dags ~/airflow/dags
```

## 📚 Documentation

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow DAG Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html)

## ⚡ Prochaines étapes

1. Démarrer Airflow: `bash ~/ETL-Yazaki/start_airflow.sh`
2. Accéder à l'interface web: http://localhost:8080
3. Vérifier que votre DAG `yazaki_etl_dag` apparaît
4. Configurer la connexion SQL Server si nécessaire
5. Activer et tester votre DAG!

Bon travail! 🎉
