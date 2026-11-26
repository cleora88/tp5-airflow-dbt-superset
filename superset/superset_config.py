# Configuration de Superset pour le TP5
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["DATABASE_URL"]

# Options : tableaux de bord par rôle, gabarits Jinja dans SQL Lab
FEATURE_FLAGS = {"DASHBOARD_RBAC": True, "ENABLE_TEMPLATE_PROCESSING": True}

# Cache des résultats des graphiques (en mémoire du serveur : suffisant pour un poste)
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 3600}

# Protéger le moteur : 10 000 lignes au plus par requête de SQL Lab
SQL_MAX_ROW = 10000
