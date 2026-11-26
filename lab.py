"""Repeatable TP commands with original stdout and measured wall time."""
from pathlib import Path
import subprocess, json, time, sys, datetime
B=Path(__file__).resolve().parent
E=B/'evidence'
def run(name,args,expect=0,timeout=1800):
    start=time.monotonic()
    print(name,flush=True)
    p=subprocess.run(args,cwd=B,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    seconds=round(time.monotonic()-start,3)
    out='$ '+' '.join(args)+'\n'+p.stdout+p.stderr
    (E/(name+'.txt')).write_text(out,encoding='utf-8')
    with (E/'commands.jsonl').open('a',encoding='utf-8') as f:
        f.write(json.dumps(dict(name=name,args=args,exit=p.returncode,seconds=seconds,time=datetime.datetime.now(datetime.timezone.utc).isoformat()))+'\n')
    print(out[-2200:],flush=True)
    if expect is not None and p.returncode!=expect:raise RuntimeError(f'{name}: exit {p.returncode}, expected {expect}')
    return p
def docker(name,*args,**kw):return run(name,['docker','compose',*args],**kw)
def dbt(name,*args,**kw):
    return docker(name,'exec','-T','-w','/opt/airflow/dbt/trajets_gold','airflow','dbt',*args,**kw)
def sql(name,q):return docker(name,'exec','-T','trino','trino','--output-format','JSON','--execute',q)
def silver(name,date):return docker(name,'exec','-T','airflow','spark-submit','--master','spark://spark-master:7077','--total-executor-cores','2','--executor-memory','1g','--class','ma.uir.tp4.SilverTrajets','/opt/airflow/jobs/tp4-spark-1.0.jar',date)
if __name__=='__main__':
    phase=sys.argv[1]
    if phase=='silver':
        silver('02-silver-01','2025-12-01');silver('02-silver-01-repeat','2025-12-01');silver('02-silver-02','2025-12-02')
        sql('02-counts','SELECT jour,count(*) AS trajets FROM lake.silver.trajets GROUP BY jour ORDER BY jour')
        sql('02-snapshots',"SELECT committed_at,operation,element_at(summary,'added-records') AS ajoutees,element_at(summary,'deleted-records') AS supprimees FROM lake.silver.\"trajets$snapshots\" ORDER BY committed_at")
    elif phase=='dbt':
        dbt('03-debug','debug')
        dbt('04-seed','seed');dbt('04-staging','run','--select','staging');dbt('04-show','show','--select','stg_trajets','--limit','5');dbt('04-freshness','source','freshness')
        dbt('05-initial','run','--vars','jour: 2025-12-02');dbt('05-merge','run','--select','trajets_jour','--vars','jour: 2025-12-02')
        dbt('05-refresh','run','--select','trajets_jour','--full-refresh','--vars','jour: 2025-12-02')
        dbt('06-tests','test');dbt('06-reconciliation','test','--select','reconciliation_carburant','--store-failures')
        sql('06-reconciliation-rows','SELECT * FROM lake.gold_dev_dbt_test__audit.reconciliation_carburant')
        seed=B/'dbt/trajets_gold/seeds/villes.csv';orig=seed.read_text(encoding='utf-8')
        try:
            seed.write_text('\n'.join(l for l in orig.splitlines() if not l.startswith('Oujda,'))+'\n',encoding='utf-8')
            p=dbt('06-build-skip','build','--vars','jour: 2025-12-02',expect=None)
            if p.returncode==0 or 'SKIP' not in p.stdout:raise RuntimeError('Expected blocking relationship failure and SKIP')
        finally:seed.write_text(orig,encoding='utf-8')
        dbt('06-restored','build','--vars','jour: 2025-12-02')
        model=B/'dbt/trajets_gold/models/marts/trajets_jour.sql';orig=model.read_text(encoding='utf-8')
        try:
            import re
            model.write_text(re.sub(r'\bas km,','as km_total,',orig),encoding='utf-8')
            p=dbt('07-contract-failure','run','--select','trajets_jour','--vars','jour: 2025-12-02',expect=None)
            if p.returncode==0 or 'contract' not in (p.stdout+p.stderr):raise RuntimeError('Expected contract failure')
        finally:model.write_text(orig,encoding='utf-8')
        dbt('07-snapshot-before','snapshot')
        sql('07-update-datsun',"UPDATE lake.silver.voitures SET conso_l100=7.10 WHERE id_voiture='0050f69aa3a26f02'")
        dbt('07-snapshot-after','snapshot')
        sql('07-snapshot-history',"SELECT modele,conso_annoncee_l100,dbt_valid_from,dbt_valid_to FROM lake.snapshots.voitures_snapshot WHERE voiture_id='0050f69aa3a26f02' ORDER BY dbt_valid_from")
        dbt('07-docs','docs','generate')
