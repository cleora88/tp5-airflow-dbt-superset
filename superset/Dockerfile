# Image Superset du TP5 : Superset 6.1.0, plus les pilotes de Trino et de PostgreSQL
FROM apache/superset:6.1.0
USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python trino psycopg2-binary
USER superset
