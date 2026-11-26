# Réponses aux questions

1. Sans la date passée par Airflow, tous les runs du backfill traiteraient la date du poste au moment de leur exécution. Les 5, 6 et 7 décembre ne seraient pas reconstruits. Le job doit utiliser la date logique du run, indépendante de l'heure réelle.

2. Avec append, un essai dont l'écriture a réussi mais dont la confirmation a échoué peut être relancé et ajouter les mêmes trajets. Le test unique sur silver.trajets.id_trajet ou stg_trajets.trajet_id détecte ces doublons. overwritePartitions remplace la journée au lieu de l'ajouter.

3. Une vue ne stocke pas son résultat : son SELECT est évalué par chaque requête qui la lit. Dans le build complet réussi de ce projet, deux marts et six tests de données lisent stg_trajets : huit lectures logiques de la vue, auxquelles peuvent s’ajouter les requêtes de métadonnées ou de validation de schéma sans lecture des données. Ce nombre change avec les modèles et tests sélectionnés. Le test unitaire utilise ses entrées simulées. Il faut envisager une table quand le coût cumulé des scans répétés dépasse celui d'une matérialisation et que la fraîcheur d'un calcul par run suffit. Le plan de Trino peut optimiser les lectures ; une création de vue ne matérialise pas les données.

4. La fenêtre recalcule le jour courant et les deux précédents pour intégrer des arrivées tardives ou des corrections récentes. Une correction de formule doit aussi toucher les journées plus anciennes : le prochain MERGE ne les relira pas. Lancer dbt run --select trajets_jour --full-refresh --target prod avec la variable jour, puis les tests et la publication après validation.

5. Avec severity=error, l'écart supérieur à 1 % du 2 décembre fait échouer le test et le build. Dans le groupe Cosmos, la réconciliation échoue et publier reste bloqué. Les tables amont déjà construites ne sont pas automatiquement annulées : dbt n'est pas une transaction globale. Le propriétaire métier et la comptabilité valident le seuil avec l'équipe data avant de rendre la règle bloquante.

6. La table trajets_jour conserve la dernière version construite avec succès : le tableau de bord continue à montrer ces chiffres, avec une fraîcheur plus ancienne. Cela évite de publier une agrégation qui aurait silencieusement perdu Oujda. Il faut afficher la date réelle de dernière publication, le propriétaire et un état de fraîcheur ; une date saisie à la main peut devenir trompeuse.

7. catchup=True aurait demandé les intervalles quotidiens depuis start_date jusqu'au dernier intervalle complet, sous réserve des limites de concurrence. catchup=False évite de lancer involontairement presque une année de traitements et de charge. Le rattrapage doit être une opération explicite et dimensionnée.

8. Le dag-processor réimporte régulièrement le fichier. Un import lourd au niveau module alourdit chaque analyse ; Variable.get au même niveau provoque des accès répétés au stockage de configuration, peut ralentir ou casser le parsing si le service est indisponible et fige la valeur dans la définition. Lire la variable dans la tâche réserve cet accès à l'exécution et prend le seuil courant.

9. Un backfill complet attendrait inutilement Bronze et recalculerait Silver alors que seul le SQL Gold a changé ; il peut aussi rejouer des livraisons. Corriger le modèle, reconstruire seulement trajets_jour en full-refresh sur les sept journées Silver déjà présentes, exécuter ses tests et sa réconciliation, puis republier et rafraîchir les consommateurs utiles. Conserver le JAR et Silver.

10. La RLS Superset ajoute un WHERE aux requêtes qu'il émet ; elle ne protège pas un accès direct à Trino, un notebook ou un autre client. Une politique au moteur ou catalogue, reliée à une identité fiable, protège tous les accès. Des identifiants partagés sans authentification, comme dans cette démonstration locale, ne suffisent pas pour une sécurité de production.

11. Il faut d'abord préciser la décision à prendre, la latence maximale utile, la fréquence d'arrivée des sources, le coût accepté et le niveau de cohérence attendu. Rafraîchir le navigateur toutes les secondes ne rend pas une source quotidienne temps réel. Si quelques minutes ont une valeur métier, étudier microbatch ou streaming, les arrivées tardives, le recalcul des agrégats, les tests, le cache et les engagements d'exploitation avant de changer l'architecture.

12. Superset sert la direction et les utilisateurs métier : indicateurs, historique des durées, compte rendu de la veille. Grafana sert l'équipe d'exploitation pendant un incident : alertes, fraîcheur et corrélation avec les journaux. Les deux lisent les métadonnées Airflow, mais leurs usages et moments de consultation diffèrent ; Superset ne remplace pas le dispositif d'alerte.

13. Une étiquette run_id crée de nouveaux flux pour chaque combinaison run, tâche et niveau. Après un an, la cardinalité augmente continuellement, ainsi que l'index, la mémoire et le coût des requêtes. task_id a un ensemble limité de valeurs ; run_id et le numéro d'essai restent des métadonnées structurées, filtrables sans multiplier les flux indexés.

14. Une ligne error peut décrire un essai transitoire réparé par un retry ; plusieurs lignes peuvent correspondre au même incident. Le callback de run écrit une alerte quand le run a réellement échoué. Une seconde règle doit détecter l'absence de succès : fraîcheur supérieure à 26 heures depuis le dernier publier réussi, avec traitement explicite du cas sans aucune publication (NULL/no data) comme alerte. Elle couvre une chaîne arrêtée qui n'écrit plus d'erreurs.
