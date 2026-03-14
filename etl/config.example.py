import pyodbc
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import os

# Configuration de la base de données source
# Copy this file to config.py and fill in your actual values
# OR use environment variables (recommended)

SERVER = os.environ.get("SQL_SERVER", r"YOUR_SERVER\INSTANCE")
DATABASE_SOURCE = os.environ.get("SQL_DB_SOURCE", "Yazaki_Source")
DATABASE_DW = os.environ.get("SQL_DB_DW", "DW_Yazaki")

SQL_USER = os.environ.get("SQL_USER", "your_sql_user")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "your_sql_password")


def get_source_connection():
    """Créer une connexion vers la base de données source SQL Server."""
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE_SOURCE};'
        f'UID={SQL_USER};'
        f'PWD={SQL_PASSWORD};'
        'Encrypt=no;TrustServerCertificate=yes'
    )

def get_dw_engine():
    """Créer un engine SQLAlchemy vers le Data Warehouse DW_Yazaki."""
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE_DW};'
        f'UID={SQL_USER};'
        f'PWD={SQL_PASSWORD};'
        'Encrypt=no;TrustServerCertificate=yes'
    )
    return create_engine(f'mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}')
