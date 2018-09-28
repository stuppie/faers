import os
import shelve
import time
from tqdm import tqdm
import requests
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from faers.settings import MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_DB, DATA_PATH

mydb = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, database=MYSQL_DB)
engine = create_engine('mysql+mysqlconnector://{}@{}/{}'.format(MYSQL_USER, MYSQL_HOST, MYSQL_DB))


def run():
    #########
    # Get all drugs and drug products
    #########

    query = """SELECT COUNT(*) AS c, DRUGNAME FROM drug_latest GROUP BY DRUGNAME"""
    drug = pd.read_sql_query(query, mydb)

    # some simple cleanup
    # strip, lowercase, fix slashes, fix ending with period
    drug['DRUGNAME_orig'] = drug.DRUGNAME
    drug.DRUGNAME = drug.DRUGNAME.str.strip().str.lower()
    drug.DRUGNAME = drug.DRUGNAME.str.replace("\\", " / ")
    drug.DRUGNAME = drug.DRUGNAME.map(lambda x: x[:-1] if x.endswith(".") else x)

    # recombine new duplicates
    drug = drug.groupby('DRUGNAME').agg({'c': sum, 'DRUGNAME_orig': lambda x: list(x)}).reset_index()
    # drug[drug.DRUGNAME_orig.map(len)>1]
    # keep only those seen 4 or more times (58k -> 17k)
    drug = drug[drug.c >= 4]
    drugs = set(drug.DRUGNAME)
    c = dict(zip(drug.DRUGNAME, drug.c))

    # export rxnconso_current table
    # https://bigquery.cloud.google.com/table/bigquery-public-data:nlm_rxnorm.rxnconso_current
    rxconso = pd.read_csv(os.path.join(DATA_PATH, "rxnconso_current.csv.gz"))
    rxconso['str'] = rxconso['str'].str.lower().str.strip()
    rxconso.drop_duplicates(subset=['rxcui', 'str'], inplace=True)
    drugname_rxcui = dict(zip(rxconso.str, rxconso.rxcui))

    ### get exact string matches
    matches = {x: drugname_rxcui[x] for x in drugs if x in drugname_rxcui}
    drug['drugname_cui'] = drug.DRUGNAME.map(lambda x: matches.get(x, ''))
    drug = drug.sort_values("c", ascending=False)

    nomatches = {x for x in drugs if x not in drugname_rxcui}
    nomatches = list(sorted(nomatches, key=c.get, reverse=True))
    print(len(matches))
    print(len(nomatches))

    ### get approx string matches
    # https://rxnav.nlm.nih.gov/RxNormAPIs.html#uLink=RxNorm_REST_getApproximateMatch
    # https://rxnav.nlm.nih.gov/RxNormApproxMatch.html
    # https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term=STOOL%20SOFTENER%20(DOCUSATE)
    with shelve.open(os.path.join(DATA_PATH, "approx_results.shelve")) as approx_results:
        for s in tqdm(nomatches[:2000]):
            if s in approx_results:
                continue
            url = "https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term={}"
            res = requests.get(url.format(s)).json()
            candidate = res['approximateGroup'].get('candidate')
            if not candidate:
                approx_results[s] = (res['approximateGroup']['inputTerm'], 0, None, None)
                continue
            top_ranked_cuis = set([int(x['rxcui']) for x in candidate if x['rank'] == "1"])
            # it sometimes returns rxcuis that don't exist (why?!)
            top_ranked_cuis = [x for x in top_ranked_cuis if x in set(rxconso.rxcui)]
            matching_str = set(rxconso[rxconso.rxcui.isin(top_ranked_cuis)]['str'])
            r = (top_ranked_cuis,
                 int(candidate[0]['score']) if candidate[0]['score'] else 0,
                 res['approximateGroup']['comment'],
                 matching_str)
            approx_results[s] = r
            time.sleep(1)

    approx_results = shelve.open(os.path.join(DATA_PATH, "approx_results.shelve"))
    approx = {k: v[0][0] for k, v in approx_results.items() if v[1] >= 67 and len(v[0]) == 1}
    drug['drugname_cui_approx'] = drug.DRUGNAME.map(lambda x: approx.get(x, ''))
    drug['drugname_cui'] = drug['drugname_cui'].combine_first(drug['drugname_cui_approx'])
    del drug['drugname_cui_approx']

    # toss those with no CUI
    drug = drug[drug.drugname_cui.map(bool)]
    # combine new duplicates cuis
    drug = drug.groupby('drugname_cui').agg({'c': sum, 'DRUGNAME_orig': sum}).reset_index()
    drug = drug.sort_values("c", ascending=False)
    # example of what this does now
    # drug[drug.drugname_cui == 632]

    # convert all CUIs down to Ingredient level
    # https://cloud.google.com/bigquery/public-data/rxnorm#what_are_the_rxcui_codes_for_the_ingredients_of_a_list_of_drugs
    rxn_all_pathways_df = pd.read_csv(os.path.join(DATA_PATH, "rxn_all_pathways_current.csv.gz"))
    rxcuis = set(drug.drugname_cui)
    dfpath = rxn_all_pathways_df[rxn_all_pathways_df.TARGET_RXCUI.isin(rxcuis)]
    dfpath_in = dfpath.query("TARGET_TTY == 'IN'")
    dfpath_min = dfpath.query("TARGET_TTY == 'MIN'")
    source_target_in = dfpath_in.groupby("SOURCE_RXCUI").TARGET_RXCUI.apply(set).to_dict()
    source_target_in = {k: list(v)[0] for k, v in source_target_in.items() if len(v) == 1}
    source_target_min = dfpath_min.groupby("SOURCE_RXCUI").TARGET_RXCUI.apply(set).to_dict()
    source_target_min = {k: list(v)[0] for k, v in source_target_min.items() if len(v) == 1}

    drug['drugname_IN_cui'] = drug['drugname_cui'].map(lambda x: source_target_in.get(x))
    drug['drugname_MIN_cui'] = drug['drugname_cui'].map(lambda x: source_target_min.get(x))
    drug['drugname_IN_MIN_cui'] = drug['drugname_MIN_cui'].combine_first(drug['drugname_IN_cui'])

    # combine new duplicates cuis
    drug = drug.groupby('drugname_IN_MIN_cui').agg({'c': sum, 'DRUGNAME_orig': sum}).reset_index()
    drug = drug.sort_values("c", ascending=False)
    drug = drug[drug.drugname_IN_MIN_cui.map(bool)]
    # example of what this does now
    # list(drug[drug.drugname_IN_MIN_cui == 632].DRUGNAME_orig)
    # list(drug[drug.drugname_IN_MIN_cui == 214182].DRUGNAME_orig)

    #https://stackoverflow.com/questions/12680754/split-explode-pandas-dataframe-string-entry-to-separate-rows
    b = pd.DataFrame(drug.DRUGNAME_orig.tolist(), index=drug.drugname_IN_MIN_cui).stack()
    b = b.reset_index()[[0, 'drugname_IN_MIN_cui']]
    b.columns = ['DRUGNAME_orig', 'drugname_IN_MIN_cui']
    b.to_sql("drug_label_mapping", engine, if_exists='replace')

    # do the mapping and store in mysql. only keeping those with matched rxnorm IDs
    s = """
    CREATE TABLE drug_latest_norm
    SELECT drugname_IN_MIN_cui, drug_latest.* from drug_latest JOIN drug_label_mapping on
    drug_latest.DRUGNAME = drug_label_mapping.DRUGNAME_orig
    """
    cursor = mydb.cursor()
    cursor.execute(s)

    cursor.execute("""alter table drug_latest_norm add index drugname_IN_MIN_cui (drugname_IN_MIN_cui)""")
    cursor.execute("""alter table drug_latest_norm add index PRIMARYID (PRIMARYID)""")
    mydb.commit()

if __name__ == "__main__":
    run()
