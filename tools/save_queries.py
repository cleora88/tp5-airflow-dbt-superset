from superset.app import create_app
app=create_app()
with app.app_context():
    from superset import db,security_manager
    from superset.models.core import Database
    from superset.models.sql_lab import SavedQuery
    lake=db.session.query(Database).filter_by(database_name='Lakehouse').one()
    admin=security_manager.find_user(username='admin')
    for label,sql in [('TP5 Silver par jour','SELECT jour, count(*) AS trajets, round(sum(distance_km)) AS km FROM lake.silver.trajets GROUP BY jour ORDER BY jour'),('TP5 Gold par jour','SELECT jour, sum(nb_trajets) AS trajets, sum(km) AS km FROM lake.gold.trajets_jour GROUP BY jour ORDER BY jour')]:
        q=db.session.query(SavedQuery).filter_by(label=label).one_or_none()
        if not q:q=SavedQuery(label=label,db_id=lake.id,user_id=admin.id,schema='gold',catalog='lake');db.session.add(q)
        q.sql=sql;q.created_by=admin;q.changed_by=admin;db.session.commit();print(q.id,label)
