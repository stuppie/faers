# faers
1. Run downloader.py
2. Run parser.py
3. Run dedupe.py
4. Run normalize_indications.py
5. Run normalize_drugs.py
6. Run get_indications.py


## Requirements

#### MySQL
Needs to be installed. Set the username and password in the `settings.py` file

#### "MRCONSO_ENG.RRF.gz". To generate:

1. Download UMLS full release
[link](https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html) and extract
2. $ zcat MRCONSO.RRF.gz| grep -F "|ENG|" | gzip > MRCONSO_ENG.RRF.gz

#### Requires `robot` available in your PATH.

Follow instructions here: http://robot.obolibrary.org/

#### Requires `mondo.owl`

Download here: http://purl.obolibrary.org/obo/mondo.owl

#### Rxnorm tables

- Requires a Google Cloud Storage account and a bucket created.
- Need [rxn_all_pathways_current](https://bigquery.cloud.google.com/table/bigquery-public-data:nlm_rxnorm.rxn_all_pathways_current)
and [rxnconso_current](https://bigquery.cloud.google.com/table/bigquery-public-data:nlm_rxnorm.rxnconso_current).
- Click "Export Table", enter your bucket name and file name. Go to the bucket and download it locally.
- Name `rxn_all_pathways_current` table -> `rxn_all_pathways_current.csv.gz`
- Name `rxnconso_current` table -> `rxnconso_current.csv.gz`

#### Data folder
Place `MRCONSO_ENG.RRF.gz`,  `mondo.owl`, `rxn_all_pathways_current.csv.gz`, `rxnconso_current.csv.gz` in `data` folder.