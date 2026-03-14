#!/bin/bash
# Script pour démarrer Airflow dans WSL

export AIRFLOW_HOME=~/airflow

echo "========================================"
echo "  Démarrage d'Airflow"
echo "========================================"
echo ""
echo "Airflow Home: $AIRFLOW_HOME"
echo "Environnement: ~/airflow-env"
echo "DAGs: ~/ETL-Yazaki/dags"
echo ""
echo "Une fois démarré, accédez à:"
echo "  http://localhost:8080"
echo ""
echo "Identifiants par défaut (mode standalone):"
echo "  Username: admin"
echo "  Password: (affiché dans les logs ci-dessous)"
echo ""
echo "Appuyez sur Ctrl+C pour arrêter Airflow"
echo "========================================"
echo ""

# Kill any previous Airflow processes
pkill -f "airflow standalone" 2>/dev/null
pkill -f "airflow scheduler" 2>/dev/null
pkill -f "airflow webserver" 2>/dev/null
sleep 2

# Démarrer Airflow en mode standalone (détaché, survit après fermeture du terminal)
nohup ~/airflow-env/bin/airflow standalone > ~/airflow/airflow.log 2>&1 &
disown

echo "Airflow en cours de démarrage..."
echo "Attendre 30 secondes puis aller sur http://localhost:8080"
echo ""
echo "Pour voir les logs: tail -f ~/airflow/airflow.log"
echo "Pour arrêter: pkill -f 'airflow standalone'"
