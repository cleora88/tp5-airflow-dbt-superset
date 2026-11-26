# Produit trajets quotidiens

Zouhair Eouichak — Groupe TP B — UIR

Le produit expose l'activité quotidienne de l'agence à la direction des opérations, aux responsables régionaux et au partenaire destinataire du CSV. Le propriétaire métier est la direction des opérations ; l'équipe data assure l'exploitation.

Le grain de `trajets_jour` est une journée UTC, une ville et un type de route. `trajets` additionne les trajets valides ; `km` additionne les distances ; la consommation est `SUM(litres)*100/NULLIF(SUM(km),0)` en l/100 km. Le nombre de voitures est distinct dans chaque groupe et ne doit pas être additionné entre groupes pour obtenir un nombre global distinct. `conso_modeles` compare la consommation réelle et annoncée par modèle. Les définitions, types, tests et lignage sont dans le projet dbt.

Airflow traite chaque nuit à minuit UTC la journée précédente : attente de Bronze, Spark Silver idempotent, contrôle des rejets, dbt Gold et tests, publication d'un Asset. L'Asset déclenche un export CSV dans MinIO. La fraîcheur promise est quotidienne ; au-delà de 26 h depuis la dernière publication réussie, l'équipe doit être alertée. Le seuil des rejets est de 5 %. La réconciliation des litres facturés est en avertissement à 1 % pendant sa calibration métier.

Superset fournit les indicateurs et filtres. Le compte `resp_fes` reçoit une règle RLS sur Fès-Meknès. Cette règle s'applique aux requêtes Superset sur `trajets_jour` ; une protection dans le moteur/catalogue est nécessaire en production. Grafana affiche la fraîcheur, les erreurs et les alertes à partir d'Airflow et de Loki ; Alloy collecte les journaux.

## Reprise locale

Les volumes MinIO et MySQL de TP4 sont réutilisés : ne pas lancer `down -v`. Démarrer Docker Desktop, construire les images avec `docker compose build`, puis `docker compose up -d`. Superset : 8088 ; Airflow : 8082 ; Trino : 8084 ; Grafana : 3000 ; Alloy : 12345 ; MinIO : 9001. PostgreSQL est publié sur 5433. Les mots de passe du sujet sont réservés à cette plateforme locale.

Le code Scala est dans `tp4-spark` ; compiler puis copier le JAR dans `jobs`. Le profil dbt de démonstration utilise Trino sans authentification. Les exports de tableaux de bord sont dans `superset/tableaux` et `grafana/tableaux`. Terminer avec `docker compose stop` ; conserver les volumes.
