from pathlib import Path
import requests,json,time,sys
from urllib.parse import quote
B=Path(__file__).resolve().parent;U='http://127.0.0.1:8082/api/v2'
def req(method,path,body=None):
    r=requests.request(method,U+path,json=body,timeout=45)
    if not r.ok:raise RuntimeError(f'{r.status_code} {r.text}')
    return r.json() if r.content else {}
def variable(value):
    body={'key':'seuil_rejet','value':str(value),'description':'Taux de rejet maximal de Silver'}
    r=requests.get(U+'/variables/seuil_rejet')
    return req('PATCH','/variables/seuil_rejet',body) if r.ok else req('POST','/variables',body)
def active(dag='trajets_quotidien',paused=False):return req('PATCH','/dags/'+dag,{'is_paused':paused})
def trigger(date):return req('POST','/dags/trajets_quotidien/dagRuns',{'dag_run_id':'manual__tp5_'+date,'logical_date':date+'T00:00:00Z'})
def clear(run,task,down=True):
    return req('POST','/dags/trajets_quotidien/clearTaskInstances',{'dry_run':False,'dag_run_id':run,'task_ids':[task],'include_downstream':down,'only_failed':False,'reset_dag_runs':True})
def runs():return req('GET','/dags/trajets_quotidien/dagRuns?limit=100&order_by=logical_date')['dag_runs']
def tasks(run):return req('GET','/dags/trajets_quotidien/dagRuns/'+quote(run,safe='')+'/taskInstances?limit=100')['task_instances']
def wait(run,timeout=1800):
    start=time.monotonic();previous=None
    while time.monotonic()-start<timeout:
        r=req('GET','/dags/trajets_quotidien/dagRuns/'+quote(run,safe=''))
        ts=tasks(run);states={s:sum(t['state']==s for t in ts) for s in set(t['state'] for t in ts)}
        summary=json.dumps({'run':run,'state':r['state'],'tasks':states})
        if summary!=previous:print(summary,flush=True);previous=summary
        if r['state'] in ['success','failed']:
            text=json.dumps({'run':r,'tasks':ts},ensure_ascii=False,indent=2)
            (B/'evidence'/('airflow-'+run.replace(':','_')+'.json')).write_text(text,encoding='utf-8')
            (B/'evidence'/('airflow-'+run.replace(':','_')+'-'+r['state']+'.json')).write_text(text,encoding='utf-8');return r
        time.sleep(5)
    raise TimeoutError(run)
if __name__=='__main__':
    command=sys.argv[1]
    if command=='setup':print(variable(.05));print(active())
    elif command=='threshold':print(variable(sys.argv[2]))
    elif command=='trigger':print(trigger(sys.argv[2]));wait('manual__tp5_'+sys.argv[2])
    elif command=='clear':print(clear(sys.argv[2],sys.argv[3]));wait(sys.argv[2])
    elif command=='wait':wait(sys.argv[2])
    elif command=='status':
        for r in runs():print(r['dag_run_id'],r['logical_date'],r['state'])
