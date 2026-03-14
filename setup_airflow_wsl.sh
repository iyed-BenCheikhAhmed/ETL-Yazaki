#!/bin/bash
# Script d'installation d'Airflow dans WSL

echo "=== Installation d'Airflow dans WSL ==="

# 1. Créer l'environnement virtuel
echo "Étape 1: Création de l'environnement virtuel..."
python3 -m venv airflow-env-linux

# 2. Activer l'environnement virtuel
echo "Étape 2: Activation de l'environnement virtuel..."
source airflow-env-linux/bin/activate

# 3. Mettre à jour pip
echo "Étape 3: Mise à jour de pip..."
pip install --upgrade pip

# 4. Installer Airflow avec les contraintes
echo "Étape 4: Installation d'Apache Airflow..."
AIRFLOW_VERSION=2.8.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 5. Installer les dépendances du projet
echo "Étape 5: Installation des dépendances du projet..."
pip install pandas pyodbc sqlalchemy

echo ""
echo "=== Installation terminée! ==="
echo ""
echo "Pour activer l'environnement virtuel à l'avenir, utilisez:"
echo "source airflow-env-linux/bin/activate"
echo ""
echo "Pour initialiser Airflow, utilisez:"
echo "airflow db init"
