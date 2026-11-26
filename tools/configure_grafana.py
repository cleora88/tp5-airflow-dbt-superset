from pathlib import Path
import json
B=Path(__file__).resolve().parents[1]
dslog={'type':'loki','uid':'loki'};dspg={'type':'grafana-postgresql-datasource','uid':'airflow-pg'}
panels=[]
def panel(id,title,type,x,y,w,h,ds,target):
    p={'id':id,'title':title,'type':type,'gridPos':{'x':x,'y':y,'w':w,'h':h},'datasource':ds,'targets':[dict(refId='A',datasource=ds,**target)],'fieldConfig':{'defaults':{},'overrides':[]},'options':{}}
    panels.append(p);return p
fresh="SELECT round(EXTRACT(EPOCH FROM now() - max(end_date))/3600,1) AS heures FROM task_instance WHERE dag_id='trajets_quotidien' AND task_id='publier' AND state='success'"
errors="SELECT t.end_date AS fin,to_char(r.logical_date,'YYYY-MM-DD') AS journée,t.task_id AS tâche,t.try_number AS essai,t.state AS état FROM task_instance t JOIN dag_run r ON r.dag_id=t.dag_id AND r.run_id=t.run_id WHERE t.dag_id='trajets_quotidien' AND t.state='failed' ORDER BY t.end_date DESC LIMIT 10"
reject='max(max_over_time({task_id="controle_qualite"} |= "taux" | regexp "taux (?P<taux>[0-9.]+)%" | unwrap taux [5m]))'
errq='sum by (task_id) (count_over_time({source="tache",level="error"}[5m]))'
p=panel(1,'Fraîcheur de Gold','stat',0,0,6,5,dspg,{'rawSql':fresh,'format':'table'});p['fieldConfig']['defaults']={'unit':'h','thresholds':{'mode':'absolute','steps':[{'color':'green','value':None},{'color':'red','value':26}]}};p['options']={'colorMode':'background','reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False}}
panel(2,'Alertes de la chaîne','logs',6,0,18,5,dslog,{'expr':'{source="alertes"}','queryType':'range'})
p=panel(3,'Taux de rejet de Silver (%)','timeseries',0,5,12,8,dslog,{'expr':reject,'queryType':'range'});p['fieldConfig']['defaults']={'unit':'percent','custom':{'drawStyle':'points','showPoints':'always','pointSize':6,'thresholdsStyle':{'mode':'line+area'}},'thresholds':{'mode':'absolute','steps':[{'color':'green','value':None},{'color':'red','value':2}]}}
p=panel(4,"Lignes d'erreur par tâche",'timeseries',12,5,12,8,dslog,{'expr':errq,'queryType':'range','legendFormat':'{{task_id}}'});p['fieldConfig']['defaults']={'custom':{'drawStyle':'bars'}}
panel(5,'Dernières tâches en échec','table',0,13,12,8,dspg,{'rawSql':errors,'format':'table'})
panel(6,"Journaux d'erreur des tâches",'logs',12,13,12,8,dslog,{'expr':'{source="tache",level="error"}','queryType':'range'})
dash={'uid':'tp5-exploitation','title':'Chaîne trajets : exploitation','tags':['TP5'],'timezone':'utc','schemaVersion':40,'version':1,'refresh':'1m','time':{'from':'now-6h','to':'now'},'panels':panels,'annotations':{'list':[]},'templating':{'list':[]}}
p=B/'grafana/tableaux/exploitation.json';p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(dash,ensure_ascii=False,indent=2),encoding='utf-8')
p=B/'grafana/provisioning/dashboards/tp5.yml';p.parent.mkdir(parents=True,exist_ok=True);p.write_text('apiVersion: 1\nproviders:\n  - name: TP5\n    folder: TP5\n    type: file\n    options:\n      path: /var/lib/grafana/dashboards\n',encoding='utf-8')
c=json.loads((B/'docker-compose.yml').read_text(encoding='utf-8'))
c['services']['grafana']['volumes'].append('./grafana/tableaux:/var/lib/grafana/dashboards:ro')
(B/'docker-compose.yml').write_text(json.dumps(c,ensure_ascii=False,indent=2),encoding='utf-8')
print('Grafana dashboard and file provisioning ready')
