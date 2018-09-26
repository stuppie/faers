import os
import subprocess
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

from faers.settings import MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_DB, DATA_PATH


def run():
    mydb = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, database=MYSQL_DB)
    engine = create_engine('mysql+mysqlconnector://{}@{}/{}'.format(MYSQL_USER, MYSQL_HOST, MYSQL_DB))

    # read umls xrefs
    names = "CUI,LAT,TS,LUI,STT,SUI,ISPREF,AUI,SAUI,SCUI,SDUI,SAB,TTY,CODE,STR,SRL,SUPPRESS,CVF,X".split(",")
    umls = pd.read_csv(os.path.join(DATA_PATH, "MRCONSO_ENG.RRF.gz"),
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

    robot_call = 'robot query --input {} --query query.sparql {}'.format(os.path.join(DATA_PATH, "mondo.owl"),
                                                                         os.path.join(DATA_PATH, "mondo.csv"))
    subprocess.check_call(robot_call, shell=True)
    os.remove("query.sparql")
    mondo = pd.read_csv(os.path.join(DATA_PATH, "mondo.csv"))
    mondo = mondo[mondo.xref.str.startswith("UMLS:")]
    mondo.xref = mondo.xref.str.replace("UMLS:", "")
    mondo.item = mondo.item.str.replace("http://purl.obolibrary.org/obo/MONDO_", "MONDO:")
    umls_mondo = dict(zip(mondo.xref, mondo.item))

    ## get all indications
    query = """SELECT * FROM indication_latest"""
    indic = pd.read_sql_query(query, mydb)

    indic['indi_pt2'] = indic.indi_pt.str.strip().str.lower()
    indic['indic_umls'] = indic.indi_pt2.map(str_umls.get)
    indic['indic_hpo'] = indic.indic_umls.map(umls_hpo.get)
    indic['indic_mondo'] = indic.indic_umls.map(umls_mondo.get)

    del indic['indi_pt2']
    del indic['caseid']
    # indic.to_csv("indications_norm.csv")
    # indic = pd.read_csv("indications_norm.csv", index_col=0)
    indic.to_sql("indication_latest_norm", engine, chunksize=10000, if_exists='replace')

    cursor = mydb.cursor()
    cursor.execute("""alter table indication_latest_norm add index PRIMARYID (PRIMARYID)""")
    cursor.execute("""alter table indication_latest_norm add index indi_drug_seq (indi_drug_seq)""")
    mydb.commit()


if __name__ == "__main__":
    run()
