# there are some cases that are in the database multiple times
# only keep the most recent instance

# example CASEID = 11927405
# select * from drug where caseid=11927405;

import pandas as pd
import mysql.connector

mydb = mysql.connector.connect(host="localhost", user="root", passwd="")
mydb.connect(database="faers")
cursor = mydb.cursor()

# this query gets the latest primary id for each case id (highest version), then gets only the drug rows
# for that primary id
# thank you stackoverflow
# https://stackoverflow.com/questions/9473718/getting-max-value-from-rows-and-joining-to-another-table
query = """
CREATE TABLE drug_latest
SELECT drug.PRIMARYID, drug.DRUG_SEQ, drug.ROLE_COD, drug.DRUGNAME, drug.PROD_AI FROM drug
  INNER JOIN (select MAX(demo.PRIMARYID) as MAXPRIMARYID from demo group by demo.CASEID) as topDemo
  ON drug.PRIMARYID = topDemo.MAXPRIMARYID
WHERE drug.ROLE_COD = ('PS')
"""
cursor.execute(query)

query = """
CREATE TABLE indication_latest
SELECT indication.* FROM indication
  INNER JOIN (select MAX(demo.PRIMARYID) as MAXPRIMARYID from demo group by demo.CASEID) as topDemo
  ON indication.PRIMARYID = topDemo.MAXPRIMARYID
"""
cursor.execute(query)

cursor.execute("""alter table drug_latest add index PRIMARYID (PRIMARYID)""")
cursor.execute("""alter table drug_latest add index DRUGNAME (DRUGNAME(10))""")
cursor.execute("""alter table drug_latest add index PROD_AI (PROD_AI(10))""")
cursor.execute("""alter table indication_latest add index PRIMARYID (PRIMARYID)""")
cursor.execute("""alter table indication_latest add index indi_drug_seq (indi_drug_seq)""")

mydb.commit()

