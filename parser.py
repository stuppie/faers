from collections import defaultdict
from os import listdir
from os.path import join, isfile
import mysql.connector
import zipfile
import re
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine

from settings import mysql_user, mysql_pass, mysql_host, mysql_db

file_name_to_table_name = {
    'THER': 'therapy',
    'RPSR': 'source',
    'REAC': 'react',
    'OUTC': 'outcome',
    'INDI': 'indication',
    'DRUG': 'drug',
    'DEMO': 'demo',
}


def create_db(dbname):
    mydb = mysql.connector.connect(host=mysql_host, user=mysql_user, passwd=mysql_pass)
    s = input('are you sure you want to drop database "{}"?: '.format(dbname))
    if s != "y":
        return False
    cursor = mydb.cursor()
    cursor.execute("DROP DATABASE IF EXISTS {}".format(dbname))
    cursor.execute("CREATE DATABASE {}".format(dbname))
    mydb.connect(database=dbname)

    # https://stackoverflow.com/questions/2108824/mysql-incorrect-string-value-error-when-save-unicode-string-in-django
    cursor = mydb.cursor()
    cursor.execute("ALTER DATABASE `%s` CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'" % dbname)
    mydb.commit()
    return True


def create_tables(valid_files):
    # read through all files once just to get the column names so we can create tables
    engine = create_engine('mysql+mysqlconnector://{}@{}/{}'.format(mysql_user, mysql_host, mysql_db))
    columns = defaultdict(set)
    for zip_files in tqdm(valid_files):
        zip_filename = zip_files[0]
        filename = zip_files[1]
        ascii_file_re = re.compile(r'as(?:c*)i(?:i*)/(.+?)\.txt', re.I)
        zip_name = ascii_file_re.search(zip_files[1]).group(1)
        if zip_name[:4] == 'STAT':
            continue
        table_name = file_name_to_table_name[zip_name[:4].upper()]
        f = zipfile.ZipFile(zip_filename, 'r')
        h = f.open(filename, 'r')
        df = pd.read_csv(h, delimiter="$", dtype=str, nrows=10)
        columns[table_name].update(set(df.columns))

    for table_name, column in columns.items():
        df = pd.DataFrame(columns=column)
        int_cols = ["primaryid", "caseid", "indi_drug_seq"]
        for int_col in int_cols:
            if int_col in df.columns:
                df[int_col] = df[int_col].astype(int)
        df.to_sql(table_name, engine, if_exists='replace', index=False)

    mydb = mysql.connector.connect(host=mysql_host, user=mysql_user, passwd=mysql_pass, database=mysql_db)
    cursor = mydb.cursor()
    cursor.execute("""alter table drug add index primaryid (primaryid)""")
    cursor.execute("""alter table drug add index role_cod (role_cod(4))""")
    cursor.execute("""alter table drug add index drugname (drugname(10))""")
    cursor.execute("""alter table drug add index prod_ai (prod_ai(10))""")
    cursor.execute("""alter table drug add index caseid (caseid)""")
    cursor.execute("""alter table indication add index primaryid (primaryid)""")
    cursor.execute("""alter table indication add index indi_drug_seq (indi_drug_seq)""")
    cursor.execute("""alter table demo add index caseid (caseid)""")
    mydb.commit()


def get_valid_files():
    files = ['data/' + f for f in listdir('data') if isfile(join('data', f)) and f[-4:].lower() == '.zip']

    for filename in files:
        if not zipfile.is_zipfile(filename):
            raise Exception(filename + ' is not a zip file')

    # Gather all the zipfiles and make sure we can get the year and quarter from
    # each file name.
    valid_files = []
    ascii_file_re = re.compile(r'as(?:c*)i(?:i*)/(.+?)\.txt', re.I)
    ascii_year_re = re.compile(r'\D(\d{4})\D')
    ascii_quarter_re = re.compile(r'q(\d)\.', re.I)
    for filename in files:
        try:
            year = int(ascii_year_re.search(filename).group(1))
            quarter = int(ascii_quarter_re.search(filename).group(1))
        except:
            raise Exception('Unable to ascertain date information for file ' + filename)
        try:
            f = zipfile.ZipFile(filename, 'r')
            f.infolist()
        except:
            raise Exception('Unable to read ' + filename)
        for name in f.namelist():
            if ascii_file_re.match(name):
                valid_files.append([filename, name, year, quarter])

    # Sort the files from past to present
    valid_files.sort(key=lambda x: (x[2], x[3]))

    return valid_files


def import_data(valid_files):
    engine = create_engine('mysql+mysqlconnector://{}@{}/{}'.format(mysql_user, mysql_host, mysql_db))
    for zip_files in tqdm(valid_files):
        zip_filename = zip_files[0]
        filename = zip_files[1]
        ascii_file_re = re.compile(r'as(?:c*)i(?:i*)/(.+?)\.txt', re.I)
        zip_name = ascii_file_re.search(zip_files[1]).group(1)
        if zip_name[:4] == 'STAT':
            continue

        table_name = file_name_to_table_name[zip_name[:4].upper()]

        f = zipfile.ZipFile(zip_filename, 'r')
        h = f.open(filename, 'r')

        df = pd.read_csv(h, delimiter="$", dtype=str)
        df = df.applymap(lambda x: x.strip()[:255] if pd.notnull(x) else x)
        df = df.applymap(lambda x: x if (x or pd.notnull(x)) else None)
        df.to_sql(table_name, engine, chunksize=20000, if_exists='append', index=False)


if __name__ == "__main__":
    assert create_db(mysql_db)

    valid_files = get_valid_files()

    create_tables(valid_files)

    import_data(valid_files)
