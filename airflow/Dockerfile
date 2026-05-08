# Télécharge l'image officielle d'Airflow avec Python 3.11
FROM apache/airflow:2.9.3-python3.11 

# ÉTAPE 1 : Installer le driver SQL Server

# Passer en root pour avoir les permissions d'installer des paquets système
USER root 

# Met à jour la liste des paquets disponibles
RUN apt-get update \
    # Installe les outils essentiels : curl (téléchargement via internet), gnupg2 (vérifier les signatures cryptographiques), unixodbc (pilote SQL)
    && apt-get install -y --no-install-recommends curl gnupg2 unixodbc unixodbc-dev \
    # Télécharge et ajoute la clé cryptographique de Microsoft (pour faire confiance à ses paquets)
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    # Ajoute le dépôt Microsoft dans les sources de paquets Linux
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    # Remet à jour pour inclure les paquets Microsoft
    && apt-get update \
    # Installe le driver ODBC Microsoft SQL Server (CRUCIAL pour la connexion à SQL Server!)
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    # Supprime le cache des téléchargements (économise de l'espace)
    && apt-get clean \
    # Supprime les listes de paquets en cache (réduit la taille de l'image)
    && rm -rf /var/lib/apt/lists/*

    # ACCEPT_EULA=Y : accepte automatiquement les termes de la licence pour le driver SQL Server, sinon l'installation échouerait en demandant une confirmation manuelle
    # -rf : force la suppression des fichiers et des dossiers, même s'ils ne sont pas vides

# ÉTAPE 2 : Installer les dépendances Python

# Passer à l'utilisateur 'airflow' pour la sécurité (pas de permissions root)
USER airflow

# Copie le fichier requirements.txt de votre machine vers le conteneur
COPY requirements.txt /requirements.txt

# Installe tous les packages Python listés dans requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

#--no-cache-dir : empêche pip de stocker les fichiers téléchargés, ce qui réduit la taille de l'image Docker

# ÉTAPE 3 : Configurer Jupyter

# Installe le kernel Jupyter pour lancer des notebooks avec cet environnement Python
RUN python -m ipykernel install --user --name python3 --display-name "Python 3"