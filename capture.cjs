const fs=require('fs'),path=require('path');
const {chromium}=require('C:/Program Files/WindowsApps/OpenAI.CodexPrimaryRuntime.v26-1123-915-0_26.1123.915.0_x64__3k8sg7r9htsxt/dependencies/node/node_modules/playwright');
const E=path.join(__dirname,'evidence');
(async()=>{
 const browser=await chromium.launch({executablePath:'C:/Program Files/Google/Chrome/Application/chrome.exe',headless:true});
 const context=await browser.newContext({viewport:{width:1500,height:1000}});const page=await context.newPage();
 const queries=[];page.on('response',async r=>{if(r.url().includes('/api/v1/chart/data')){try{const d=await r.json();queries.push({url:r.url(),status:r.status(),result:d.result?.map(x=>({query:x.query,data:x.data,rowcount:x.rowcount,status:x.status}))});}catch(e){}}});
 try{
  const mode=process.argv[2];
  if(mode==='outputs'){
   for(const f of fs.readdirSync(E).filter(f=>/^\d.*\.txt$/.test(f))){
    let text=fs.readFileSync(path.join(E,f),'utf8').replace(/\u001b\[[0-?]*[ -/]*[@-~]/g,'');
    text=text.split('\n').filter(l=>!/^\d{2}\/\d{2}\/\d{2} .* (INFO|WARN) /.test(l)).join('\n');
    if(text.length>18000)text=text.slice(0,1800)+'\n[…]\n'+text.slice(-15000);
    await page.setContent('<style>body{padding:28px;margin:0;background:#152431;color:#f6f7fa}h1{font:600 22px Arial}pre{font:15px/1.45 Consolas;white-space:pre-wrap;overflow-wrap:anywhere}</style><h1></h1><pre></pre>');
    await page.locator('h1').evaluate((el,f)=>el.textContent='Sortie réelle enregistrée — '+f,f);
    await page.locator('pre').evaluate((el,s)=>el.textContent=s,text);
    await page.screenshot({path:path.join(E,f.replace('.txt','-output.png')),fullPage:true});
   }
  }else{
   const url=process.argv[3],name=process.argv[4];
   if(mode==='superset'||mode==='sql'){
    const user=process.argv[5]||'admin';await page.goto('http://localhost:8088/login/');
    await page.locator('#username').fill(user);await page.locator('#password').fill(user);await page.locator('input[type=submit],button[type=submit]').first().click();
   }
   if(mode==='grafana'){
    const login=await context.request.post('http://localhost:3000/login',{data:{user:'admin',password:'admin'}});
    if(!login.ok())throw new Error('Grafana login '+login.status()+' '+await login.text());
   }
   if(mode==='trino'){
    await page.goto('http://localhost:8084');await page.locator('input').first().fill('zouhair');await page.getByRole('button',{name:'Log in',exact:true}).click();await page.waitForTimeout(1200);
   }
   if(mode==='airflow-grid')await page.setViewportSize({width:1100,height:900});
   if(mode==='airflow-graph')await page.setViewportSize({width:1800,height:1100});
   await page.goto(url,{waitUntil:'domcontentloaded'});await page.waitForTimeout(6500);
   if(mode==='sql'){await page.locator('[data-test=run-query-action]').click();await page.waitForTimeout(4500);}
   if(mode==='airflow-grid'||mode==='airflow-graph'){
    await page.getByRole('button',{name:mode==='airflow-grid'?'Show Grid (Press g)':'Show Graph (Press g)',exact:true}).click();
    await page.getByRole('button',{name:'Expand all task groups',exact:true}).click();await page.waitForTimeout(1500);
    const collapse=page.getByRole('button',{name:'Collapse Details Panel',exact:true});if(await collapse.isVisible())await collapse.click();await page.waitForTimeout(700);
   }
   console.log((await page.locator('body').innerText()).slice(0,8000));
   await page.screenshot({path:path.join(E,name),fullPage:true});
   fs.writeFileSync(path.join(E,name.replace('.png','.html')),await page.content());
   if(queries.length)fs.writeFileSync(path.join(E,name.replace('.png','-queries.json')),JSON.stringify(queries,null,2));
  }
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
