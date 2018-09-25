import pickle
import pandas as pd
import mysql.connector
from tqdm import tqdm

from settings import mysql_user, mysql_pass, mysql_host, mysql_db

mydb = mysql.connector.connect(host=mysql_host, user=mysql_user, passwd=mysql_pass, database=mysql_db)
cursor = mydb.cursor()

top_indic_query = """
SELECT
  COUNT(*)           AS count,
  indication.INDI_PT AS 'indication_name',
  indication.indic_umls,
  indication.indic_hpo,
  indication.indic_mondo
FROM indication_latest_norm as indication
  LEFT JOIN drug_latest_norm as drug ON drug.primaryid = indication.primaryid AND
                    indication.indi_drug_seq = drug.drug_seq
WHERE drug.role_cod = ('PS') AND drug.drugname_in_cui = ({})
GROUP BY indication_name,indication.indic_umls, indication.indic_hpo, indication.indic_mondo having count > 20
ORDER BY count DESC
"""

all_drugs_query = """select drug.drugname_in_cui,
GROUP_CONCAT(DISTINCT(drug.drugname) SEPARATOR '|') as drug_names
from drug_latest_norm as drug
group by drug.drugname_in_cui"""
drug_df = pd.read_sql_query(all_drugs_query, mydb)
drug_cui_to_labels = dict(zip(drug_df.drugname_in_cui, drug_df.drug_names))
all_drugs = set(list(drug_df.drugname_in_cui))

indications = dict()
for drug_cui in tqdm(all_drugs):
    this_query = top_indic_query.format(drug_cui)
    top = pd.read_sql_query(this_query, mydb)
    top = top.dropna(subset=['indic_mondo'])
    indications[drug_cui] = top

with open("indications.pkl", 'wb') as f:
    pickle.dump(indications, f)
indications = pickle.load(open("indications.pkl", 'rb'))

indic_list = [
    {
        "drug_labels": drug_cui_to_labels[k],
        "drug_rxcui": k,
        "indications_mondo": "|".join(list(df.indic_mondo)),
        "indications_umls": "|".join(list(df.indic_umls)),
        "indications_label": "|".join(list(df.indication_name)),
    } for k, df in indications.items()]
indic_df = pd.DataFrame(indic_list)
indic_df.to_csv("faers_indications.csv")
