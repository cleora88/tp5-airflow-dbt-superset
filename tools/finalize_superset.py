from superset.app import create_app
import json
app=create_app()
with app.app_context():
    from superset import db,security_manager
    from superset.models.dashboard import Dashboard
    admin=security_manager.find_user(username='admin')
    d=db.session.query(Dashboard).filter_by(slug='trajets-suivi').one()
    pos=json.loads(d.position_json)
    for item in pos.values():
        if isinstance(item,dict) and item.get('type')=='MARKDOWN':
            item['meta']['code']='**Données au 9 décembre 2025**, recalculées chaque nuit par Airflow (`trajets_quotidien`).\n\nPropriétaire : direction des opérations. Définitions : documentation dbt, modèle `trajets_jour`.'
        if isinstance(item,dict) and item.get('type')=='CHART' and item.get('meta',{}).get('chartId')==5:item['meta']['height']=80
    d.position_json=json.dumps(pos,ensure_ascii=False);d.created_by=admin;d.changed_by=admin
    db.session.commit();print('Final dashboard date and table height updated')
