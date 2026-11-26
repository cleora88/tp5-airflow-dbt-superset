"""Run inside the Superset container after db upgrade and init."""
import json, uuid
from superset.app import create_app
app=create_app()
with app.app_context():
    from superset import db, security_manager as sm
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable, SqlMetric, RowLevelSecurityFilter
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    admin=sm.find_user(username='admin')
    def database(name,uri):
        d=db.session.query(Database).filter_by(database_name=name).one_or_none()
        if not d:d=Database(database_name=name);db.session.add(d)
        d.sqlalchemy_uri=uri;d.expose_in_sqllab=True;d.allow_dml=False;d.cache_timeout=3600
        db.session.commit();return d
    lake=database('Lakehouse','trino://superset@trino:8080/lake')
    pg=database('Airflow (métadonnées)','postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    def dataset(database,name,schema=None,sql=None):
        t=db.session.query(SqlaTable).filter_by(database_id=database.id,table_name=name,schema=schema).one_or_none()
        if not t:t=SqlaTable(table_name=name,schema=schema,database=database,owners=[admin]);db.session.add(t)
        if sql:t.sql=sql
        t.description='Produit trajets quotidiens — Direction des opérations. Définitions dans dbt.'
        t.certified_by='Équipe data, contrat dbt v1';t.certification_details='Indicateurs construits et testés par dbt.'
        db.session.commit();t.fetch_metadata();db.session.commit();return t
    trajets=dataset(lake,'trajets_jour','gold');modeles=dataset(lake,'conso_modeles','gold')
    for c in trajets.columns:
        if c.column_name=='jour':c.is_dttm=True;trajets.main_dttm_col='jour'
    for name,expr,label in [('trajets','SUM(nb_trajets)','Trajets'),('km','SUM(km)','Kilomètres'),('conso_l100','SUM(litres)*100/NULLIF(SUM(km),0)','Consommation (l/100 km)')]:
        m=next((m for m in trajets.metrics if m.metric_name==name),None)
        if not m:m=SqlMetric(metric_name=name);trajets.metrics.append(m)
        m.expression=expr;m.verbose_name=label
    db.session.commit()
    runs_sql="""SELECT r.run_id,r.run_type,r.state,to_char(r.logical_date,'YYYY-MM-DD') AS jour_traite,min(t.start_date) AS debut,EXTRACT(EPOCH FROM max(t.end_date)-min(t.start_date)) AS duree_s,count(*) FILTER(WHERE t.state IN ('failed','upstream_failed')) AS taches_ko FROM dag_run r JOIN task_instance t ON t.dag_id=r.dag_id AND t.run_id=r.run_id WHERE r.dag_id='trajets_quotidien' AND r.end_date IS NOT NULL GROUP BY r.run_id,r.run_type,r.state,r.logical_date"""
    runs=dataset(pg,'runs_trajets',sql=runs_sql)
    def chart(title,typ,t,**extra):
        tid=t.id
        c=db.session.query(Slice).filter_by(slice_name=title).one_or_none()
        if not c:c=Slice(slice_name=title,owners=[admin],datasource_id=tid,datasource_type='table',viz_type=typ);db.session.add(c)
        c.datasource_id=tid;c.datasource_type='table';c.viz_type=typ
        form=dict(datasource=f'{t.id}__table',viz_type=typ,time_range='No filter',adhoc_filters=[],row_limit=10000,color_scheme='supersetColors')
        form.update(extra)
        c.params=json.dumps(form,ensure_ascii=False);db.session.commit();return c
    charts=[chart('Trajets par jour','big_number',trajets,metric='trajets',granularity_sqla='jour',time_grain_sqla='P1D',show_trend_line=True,y_axis_format=',d'),
            chart('Consommation réelle','big_number_total',trajets,metric='conso_l100',y_axis_format='.2f',subheader='l/100 km',header_font_size=0.5,subheader_font_size=0.18),
            chart('Kilomètres par type de route','echarts_timeseries_line',trajets,x_axis='jour',granularity_sqla='jour',time_grain_sqla='P1D',metrics=['km'],groupby=['type_route'],show_legend=True,x_axis_time_format='%d/%m'),
            chart('Trajets par région','echarts_timeseries_bar',trajets,x_axis='region',metrics=['trajets'],groupby=[],orientation='horizontal',sort_series_type='sum',sort_series_ascending=False,show_legend=False,order_desc=True,sort_x_axis=True,x_axis_sort='trajets',x_axis_sort_asc=False),
            chart('Écart à la conso annoncée','table',modeles,query_mode='raw',all_columns=['modele','origine','nb_trajets','conso_annoncee_l100','conso_reelle_l100','ecart_pct'],order_by_cols=[json.dumps(['ecart_pct',False])],row_limit=15,table_timestamp_format='smart_date')]
    # row_limit is supplied explicitly for the raw-record table.
    def layout(cs,text=None):
        pos={'DASHBOARD_VERSION_KEY':'v2','ROOT_ID':{'id':'ROOT_ID','type':'ROOT','children':['GRID_ID']},'GRID_ID':{'id':'GRID_ID','type':'GRID','children':[],'parents':['ROOT_ID']}}
        rows=[]
        if text:
            rows.append([('MARKDOWN',text,12)])
        rows.extend([[('CHART',cs[0],6),('CHART',cs[1],6)]] if len(cs)>1 else [])
        if len(cs)>2:rows.append([('CHART',cs[2],12)])
        if len(cs)>3:rows.extend([[('CHART',cs[3],12)],[('CHART',cs[4],12)]])
        for idx,row in enumerate(rows):
            rid=f'ROW-{idx}';pos['GRID_ID']['children'].append(rid)
            pos[rid]={'id':rid,'type':'ROW','children':[],'parents':['ROOT_ID','GRID_ID'],'meta':{'background':'BACKGROUND_TRANSPARENT'}}
            for j,(kind,value,w) in enumerate(row):
                cid=f'{kind}-{idx}-{j}';pos[rid]['children'].append(cid)
                meta={'width':w,'height':(28 if value in cs[:2] else 35 if value.viz_type!='table' else 62),'chartId':value.id,'sliceName':value.slice_name} if kind=='CHART' else {'width':w,'height':12,'code':value}
                pos[cid]={'id':cid,'type':kind,'parents':['ROOT_ID','GRID_ID',rid],'children':[],'meta':meta}
        return pos
    def dashboard(title,slug,cs,text=None):
        d=db.session.query(Dashboard).filter_by(slug=slug).one_or_none()
        if not d:d=Dashboard(dashboard_title=title,slug=slug,owners=[admin]);db.session.add(d)
        d.published=True;d.slices=cs;d.position_json=json.dumps(layout(cs,text),ensure_ascii=False)
        d.json_metadata=json.dumps({'refresh_frequency':0,'color_scheme':'supersetColors','timed_refresh_immune_slices':[],'expanded_slices':{},'native_filter_configuration':[]})
        db.session.commit();return d
    d=dashboard('Trajets : suivi quotidien','trajets-suivi',charts,'**Données au 7 décembre 2025**, recalculées chaque nuit par Airflow (`trajets_quotidien`).\n\nPropriétaire : direction des opérations. Définitions : documentation dbt, modèle `trajets_jour`.')
    filters=[]
    for name,col,typ in [('Région','region','filter_select'),('Type de route','type_route','filter_select'),('Période',None,'filter_time')]:
        filters.append({'id':'NATIVE_FILTER-'+str(uuid.uuid4()),'name':name,'filterType':typ,'targets':[{'datasetId':trajets.id,'column':{'name':col}}] if col else [{}],'defaultDataMask':{'extraFormData':{},'filterState':{},'ownState':{}},'cascadeParentIds':[],'scope':{'rootPath':['ROOT_ID'],'excluded':[charts[4].id]},'controlValues':{'enableEmptyFilter':False,'multiSelect':True,'searchAllOptions':False,'inverseSelection':False},'type':'NATIVE_FILTER'})
    meta=json.loads(d.json_metadata);meta['native_filter_configuration']=filters;d.json_metadata=json.dumps(meta,ensure_ascii=False)
    duration={'expressionType':'SQL','sqlExpression':'MAX(duree_s)','label':'Durée (s)','optionName':'metric_duree'}
    supcharts=[chart('Durée des runs (s)','echarts_timeseries_bar',runs,x_axis='jour_traite',metrics=[duration],groupby=['state'],show_legend=True),chart('Runs en échec','table',runs,query_mode='raw',all_columns=['jour_traite','run_type','state','taches_ko'],adhoc_filters=[{'expressionType':'SIMPLE','subject':'state','operator':'==','comparator':'failed','clause':'WHERE','isExtra':False,'isNew':False,'sqlExpression':None,'filterOptionName':'failed'}])]
    dashboard('Chaîne trajets : supervision','trajets-supervision',supcharts)
    role=sm.add_role('Region_Fes_Meknes')
    for t in [trajets,modeles]:
        pvm=sm.add_permission_view_menu('datasource_access',t.perm)
        if pvm not in role.permissions:role.permissions.append(pvm)
    gamma=sm.find_role('Gamma')
    user=sm.find_user(username='resp_fes')
    if not user:user=sm.add_user('resp_fes','Responsable','Fès','resp_fes@tp5.local',[gamma,role],password='resp_fes')
    else:user.roles=[gamma,role]
    rls=db.session.query(RowLevelSecurityFilter).filter_by(name='region_fes_meknes').one_or_none()
    if not rls:rls=RowLevelSecurityFilter(name='region_fes_meknes',filter_type='Regular',clause="region = 'Fès-Meknès'");db.session.add(rls)
    rls.filter_type='Regular';rls.clause="region = 'Fès-Meknès'";rls.tables=[trajets];rls.roles=[role]
    d.roles=[role];db.session.commit()
    print(json.dumps({'dashboard':d.id,'charts':[c.id for c in charts],'trajets_dataset':trajets.id,'modeles_dataset':modeles.id,'runs_dataset':runs.id},ensure_ascii=False))
