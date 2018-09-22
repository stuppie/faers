import pandas as pd

dcdf = pd.read_csv("drugcentral/indications_drugcentral.csv")
dcdf = dcdf.query("id_type == 'RXNORM'")
dcdf = dcdf.groupby("identifier").agg({'umls_cui': list, 'concept_name': list}).reset_index()
dcdf.rename(columns={'identifier': 'drug_rxcui',
                     'umls_cui': 'indications_umls',
                     'concept_name': 'indications_label'}, inplace=True)
dcdf = dcdf.set_index("drug_rxcui")
dcdf.indications_umls = dcdf.indications_umls.map(lambda x:"|".join(x))
dcdf.indications_label = dcdf.indications_label.map(lambda x:"|".join(x))

faersdf = pd.read_csv("faers_indications.csv", index_col=0, dtype=str).dropna()
faersdf = faersdf.set_index("drug_rxcui")

faersdf_join = faersdf.join(dcdf, rsuffix="_dc")
faersdf_join.to_csv("faers_drugcentral_indications.csv")


#faersdf.indications_umls = faersdf.indications_umls.map(lambda x: x.split("|"))


dc = dict(zip(dcdf.drug_rxcui.astype(str), dcdf.indications_umls))
faers = dict(zip(faersdf.drug_rxcui.astype(str), faersdf.indications_umls))

# toss faers drugs that aren't in drugcentral, as we have no way of evaluating them
faers = {k: v for k, v in faers.items() if k in dc}

# toss dc drugs not in faers
dc = {k: v for k, v in dc.items() if k in faers}

"""
TP = []
TN = []
FP = []
FN = []
p = []
r = []
"""
TP = 0
TN = 0
FP = 0
FN = 0
p = 0
r = 0
for k, dc_indications in dc.items():
    faers_indications = set(faers.get(k, []))
    dc_indications = set(dc_indications)
    tp = len(dc_indications & faers_indications)
    fp = len(faers_indications - dc_indications)
    fn = len(dc_indications - faers_indications)
    TP += tp
    FP += fp
    FN += fn
    #TP.append(tp)
    #FP.append(fp)
    #FN.append(fn)
    #p.append(tp / (tp + fp))
    #r.append(tp / (tp + fn))


precision = TP / (TP + FP)
recall = TP / (TP + FN)
print(precision)
print(recall)
