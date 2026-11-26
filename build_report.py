from pathlib import Path
from html import escape
import json,re
from PIL import Image as PILImage
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Image,PageBreak,Table,TableStyle,KeepTogether
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
B=Path(__file__).resolve().parent;E=B/'evidence'
for n,f in [('Body','segoeui.ttf'),('Bold','segoeuib.ttf'),('Mono','consola.ttf')]:pdfmetrics.registerFont(TTFont(n,'C:/Windows/Fonts/'+f))
pdfmetrics.registerFontFamily('Body',normal='Body',bold='Bold')
styles=getSampleStyleSheet()
for n in ['Normal','BodyText']:styles[n].fontName='Body';styles[n].fontSize=9.5;styles[n].leading=13;styles[n].spaceAfter=7
for n in ['Title','Heading1','Heading2']:styles[n].fontName='Bold';styles[n].textColor=colors.black
styles['Title'].fontSize=27;styles['Title'].leading=33
styles.add(ParagraphStyle('Caption',fontName='Body',fontSize=8,leading=11,spaceAfter=10,textColor=colors.HexColor('#475569')))
styles.add(ParagraphStyle('Small',fontName='Body',fontSize=8,leading=11,spaceAfter=6))
story=[]
def p(text,style='BodyText'):story.append(Paragraph(text,styles[style]))
def h(text):p(text,'Heading1')
def shot(name,caption,maxh=440):
    f=E/name
    with PILImage.open(f) as im:w,hh=im.size
    scale=min(495/w,maxh/hh);story.append(Image(str(f),width=w*scale,height=hh*scale,hAlign='CENTER'));p(caption,'Caption')
def new(title):story.append(PageBreak());h(title)
v=json.loads((E/'final-validation.json').read_text(encoding='utf-8'))
logo=Path(r'C:\Users\User\Documents\UIR\UIR_logo.png')
with PILImage.open(logo) as im:w,hh=im.size
story.append(Spacer(1,35));story.append(Image(str(logo),width=230,height=230*hh/w,hAlign='CENTER'));story.append(Spacer(1,60))
p('TP5 Orchestrer transformer<br/>exposer et surveiller<br/>le lakehouse','Title')
p('Technologies Big Data II','Heading2');p('Airflow · dbt · Trino · Superset · Grafana')
story.append(Spacer(1,45));p('<b>Zouhair Eouichak</b><br/>Groupe TP B<br/>Université Internationale de Rabat')
p('Pr Youssef GAHI')
new('Compte rendu du fonctionnement')
for text in v['chronology_paragraphs']:p(text)
rows=[['Tâche ou groupe','Durée mesurée','Résultat']]+v['normal_run_timings']
t=Table(rows,colWidths=[240,100,155],repeatRows=1,hAlign='LEFT')
t.setStyle(TableStyle([('FONTNAME',(0,0),(-1,0),'Bold'),('FONTNAME',(0,1),(-1,-1),'Body'),('FONTSIZE',(0,0),(-1,-1),8),('LEADING',(0,0),(-1,-1),11),('BACKGROUND',(0,0),(-1,0),colors.HexColor('#173650')),('TEXTCOLOR',(0,0),(-1,0),colors.white),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f0f4f8')]),('GRID',(0,0),(-1,-1),.4,colors.HexColor('#D9D9D9')),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('LEFTPADDING',(0,0),(-1,-1),8),('RIGHTPADDING',(0,0),(-1,-1),8),('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7)]));story.append(t)
for text in v['incident_paragraphs']:p(text)
new('Réponses aux questions')
answers=(B/'report/answers.md').read_text(encoding='utf-8').split('\n\n')[1:]
for text in answers:
    if not text.strip():continue
    m=re.match(r'(\d+)\. (.*)',text,re.S)
    p('<b>Question '+m[1]+'</b><br/>'+escape(m[2]).replace('\n',' ') if m else escape(text))
for section in v['evidence_sections']:
    new(section['title'])
    for text in section.get('paragraphs',[]):p(text)
    for entry in section['images']:shot(entry['file'],entry['caption'],entry.get('max_height',440))
new('Reproductibilité et livraison')
for text in v['delivery_paragraphs']:p(text)
p('Sources : sujet TP5 Airflow dbt Superset et chapitre 6 bis dbt fournis ; documentation officielle Trino 484 pour fs.s3.enabled. Les versions, sorties, requêtes, captures et durées de ce poste sont conservées dans le dossier de preuves.','Small')
out=B/'TP5_Eouichak_Zouhair.pdf'
def footer(canvas,doc):
    if doc.page>1:
        canvas.setFont('Body',8);canvas.setFillColor(colors.HexColor('#64748b'));canvas.drawString(50,27,'Zouhair Eouichak · TP B · TP5 Technologies Big Data');canvas.drawRightString(545,27,str(doc.page))
SimpleDocTemplate(str(out),pagesize=A4,rightMargin=50,leftMargin=50,topMargin=45,bottomMargin=45,title='TP5 Airflow dbt Superset Grafana',author='Zouhair Eouichak').build(story,onFirstPage=footer,onLaterPages=footer)
print(out)
