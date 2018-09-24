import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

from settings import mysql_user, mysql_pass, mysql_host, mysql_db

mydb = mysql.connector.connect(host=mysql_host, user=mysql_user, passwd=mysql_pass, database=mysql_db)
engine = create_engine('mysql+mysqlconnector://{}@{}/{}'.format(mysql_user, mysql_host, mysql_db))

# download umls data
names = "CUI,LAT,TS,LUI,STT,SUI,ISPREF,AUI,SAUI,SCUI,SDUI,SAB,TTY,CODE,STR,SRL,SUPPRESS,CVF,X".split(",")
umls = pd.read_csv("MRCONSO_ENG.RRF.gz",
                   delimiter="|", names=names, index_col=None, dtype=str,
                   usecols=['CUI', 'CODE', 'STR', 'SAB', 'LAT'])

umls.STR = umls.STR.str.lower()
umls.drop_duplicates(subset=['CUI', 'CODE', 'STR'], inplace=True)
umls_mdr = umls[umls.SAB == "MDR"]
umls_hpo = umls[umls.SAB == "HPO"]

str_umls = dict(zip(umls_mdr.STR, umls_mdr.CUI))
umls_hpo = dict(zip(umls_hpo.CUI, umls_hpo.CODE))

# mondo xrefs
s = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>

SELECT * WHERE {
  ?item oboInOwl:hasDbXref ?xref
}
"""
with open("query.sparql", 'w') as f:
    f.write(s)
os.system('robot query --input mondo.owl --query query.sparql mondo.csv')
mondo = pd.read_csv("mondo.csv")
mondo = mondo[mondo.xref.str.startswith("UMLS:")]
mondo.xref = mondo.xref.str.replace("UMLS:", "")
mondo.item = mondo.item.str.replace("http://purl.obolibrary.org/obo/MONDO_", "MONDO:")
umls_mondo = dict(zip(mondo.xref, mondo.item))

## get all indications
query = """SELECT * FROM indication_latest"""
indic = pd.read_sql_query(query, mydb)

indic['INDI_PT2'] = indic.INDI_PT.str.strip().str.lower()
indic['indic_umls'] = indic.INDI_PT2.map(str_umls.get)
indic['indic_hpo'] = indic.indic_umls.map(umls_hpo.get)
indic['indic_mondo'] = indic.indic_umls.map(umls_mondo.get)

del indic['INDI_PT2']
del indic['ISR']
del indic['DRUG_SEQ']
del indic['CASEID']
# indic.to_csv("indications_norm.csv")
# indic = pd.read_csv("indications_norm.csv", index_col=0)
indic.to_sql("indication_latest_norm", engine, chunksize=10000, if_exists='replace')

cursor = mydb.cursor()
cursor.execute("""alter table indication_latest_norm add index PRIMARYID (PRIMARYID)""")
cursor.execute("""alter table indication_latest_norm add index indi_drug_seq (indi_drug_seq)""")
mydb.commit()
