from pathlib import Path
import requests,json
B=Path(__file__).resolve().parents[1]
s=requests.Session();s.auth=('admin','admin')
u='http://127.0.0.1:3000'
r=s.get(u+'/api/folders/tp5')
if r.status_code==404:
    r=s.post(u+'/api/folders',json={'uid':'tp5','title':'TP5'});r.raise_for_status()
rule={'uid':'tp5-chaine-failed','orgID':1,'folderUID':'tp5','ruleGroup':'chaine','title':'Chaîne trajets en échec','condition':'C','for':'0s','noDataState':'OK','execErrState':'Error','annotations':{'summary':"Un run de trajets_quotidien a échoué : voir le tableau de bord d'exploitation."},'labels':{'projet':'TP5'},'isPaused':False,'data':[
 {'refId':'A','queryType':'','relativeTimeRange':{'from':600,'to':0},'datasourceUid':'loki','model':{'expr':'sum(count_over_time({source="alertes"} [10m]))','instant':True,'queryType':'instant','intervalMs':1000,'maxDataPoints':43200,'refId':'A'}},
 {'refId':'B','queryType':'','relativeTimeRange':{'from':0,'to':0},'datasourceUid':'__expr__','model':{'type':'reduce','expression':'A','reducer':'last','settings':{'mode':'dropNN'},'refId':'B','intervalMs':1000,'maxDataPoints':43200}},
 {'refId':'C','queryType':'','relativeTimeRange':{'from':0,'to':0},'datasourceUid':'__expr__','model':{'type':'threshold','expression':'B','refId':'C','conditions':[{'evaluator':{'params':[0],'type':'gt'},'operator':{'type':'and'},'query':{'params':['C']},'reducer':{'params':[],'type':'last'},'type':'query'}]}}
]}
r=s.get(u+'/api/v1/provisioning/alert-rules/tp5-chaine-failed')
r=s.put(u+'/api/v1/provisioning/alert-rules/tp5-chaine-failed',json=rule) if r.ok else s.post(u+'/api/v1/provisioning/alert-rules',json=rule)
print(r.status_code,r.text[:1500]);r.raise_for_status()
(B/'grafana/tableaux/alerte.json').write_text(json.dumps(r.json(),ensure_ascii=False,indent=2),encoding='utf-8')
