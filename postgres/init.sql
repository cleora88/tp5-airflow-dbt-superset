-- Exécuté une seule fois, au premier démarrage de postgres : une base par application
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset OWNER superset;
