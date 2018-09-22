import os
import pandas as pd
from io import StringIO

# example getting IN from whatever
query = """
SELECT * FROM `bigquery-public-data.nlm_rxnorm.rxn_all_pathways_current` WHERE
  REGEXP_CONTAINS(SOURCE_NAME, "(?i)(^(Vancomycin 100 MG/ML|ENBREL|Valium))")
  AND TARGET_TTY = 'IN'
"""

# https://cloud.google.com/bigquery/public-data/rxnorm#what_are_the_rxcui_codes_for_the_ingredients_of_a_list_of_drugs
def get_tty_df_from_cuis(cuis):
    query = """
    SELECT * FROM `bigquery-public-data.nlm_rxnorm.rxn_all_pathways_current` WHERE
      SOURCE_RXCUI IN ({})
      AND TARGET_TTY IN ("MIN", "IN")
    """.format(",".join(['"' + str(x) + '"' for x in cuis]))
    s = os.popen("echo '{}' | bq query --use_legacy_sql=false --format=csv -n 999999999".format(query)).read()
    res = pd.read_csv(StringIO(s))
    return res
