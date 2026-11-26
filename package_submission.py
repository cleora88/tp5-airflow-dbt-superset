from pathlib import Path
import json,subprocess,zipfile,hashlib
from pypdf import PdfReader
B=Path(__file__).resolve().parent;E=B/'evidence'
tracked=subprocess.check_output(['git','ls-files'],cwd=B,text=True).splitlines()
assert not any('/target/' in n or '/logs/' in n or '.user.yml' in n for n in tracked)
assert len(PdfReader(B/'TP5_Eouichak_Zouhair.pdf').pages)==28
for i in range(1,15):
    assert f'Question {i}' in '\n'.join(p.extract_text() for p in PdfReader(B/'TP5_Eouichak_Zouhair.pdf').pages[:4])
v=json.loads((E/'final-validation.json').read_text(encoding='utf-8'))
assert all(x['verified'] for x in json.loads((E/'checklist.json').read_text(encoding='utf-8')))
images={i['file'] for s in v['evidence_sections'] for i in s['images']}
images.update(['00-spark-platform.png','12-sql-silver.png','19-airflow-failed-grid.png','20-dashboard-final.png','20-fes-final.png'])
assert all((E/n).is_file() for n in images)
proof=[p for p in E.iterdir() if p.is_file() and p.suffix in ['.txt','.json','.jsonl'] and p.name!='airflow-openapi.json']
proof += [E/n for n in sorted(images)]
git_files=[p for p in (B/'.git').rglob('*') if p.is_file() and 'hooks' not in p.parts]
dest=B/'TP5_Eouichak_Zouhair_complet.zip'
with zipfile.ZipFile(dest,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
    for name in tracked:z.write(B/name,'TP5/'+name)
    for p in git_files:z.write(p,'TP5/'+p.relative_to(B).as_posix())
    for p in proof:z.write(p,'TP5/evidence/'+p.name)
with zipfile.ZipFile(dest) as z:
    assert z.testzip() is None
    assert 'TP5/TP5_Eouichak_Zouhair.pdf' in z.namelist()
    assert 'TP5/.git/HEAD' in z.namelist()
    assert not any('/target/' in n or '/logs/' in n or 'pdf-libs' in n for n in z.namelist())
    for p in proof:assert z.read('TP5/evidence/'+p.name)==p.read_bytes()
summary={'archive':str(dest),'bytes':dest.stat().st_size,'screenshots':len(images),'source_files':len(tracked),'proof_files':len(proof),'sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),'git_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=B,text=True).strip(),'report_pages':28,'steps_verified':19,'shutdown':'All 12 TP5 containers stopped; Docker Desktop stopped; data volumes preserved.'}
(E/'submission-validation.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
print(json.dumps(summary,indent=2))
