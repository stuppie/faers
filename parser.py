"""
Parse module for parsing the FDA's FEARS "$" delimitted files.
Modified from: https://github.com/wizzl35/faers-data

"""
import functools
from os import listdir
from os.path import join, isfile
import mysql.connector
import zipfile
import re


class DB:
    def __init__(self):
        # Import DB module and create DB structure
        self.mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd=""
        )
        s = input("are you sure you want to drop database?: ")
        if s != "y":
            return
        cursor = self.mydb.cursor()
        cursor.execute("DROP DATABASE IF EXISTS faers")
        cursor.execute("CREATE DATABASE faers")
        self.mydb.connect(database="faers")
        cursor = self.mydb.cursor()

        # create tables
        cursor.execute("""
        create table demo (ISR VARCHAR(255), PRIMARYID VARCHAR(255), CASEID VARCHAR(255), CASEVERSION VARCHAR(255),
        CASE_NUM VARCHAR(255), I_F_COD VARCHAR(255), FOLL_SEQ VARCHAR(255), IMAGE VARCHAR(255), EVENT_DT VARCHAR(255),
        MFR_DT VARCHAR(255), INIT_FDA_DT VARCHAR(255), FDA_DT VARCHAR(255), REPT_COD VARCHAR(255), AUTH_NUM VARCHAR(255),
        MFR_NUM VARCHAR(255), MFR_SNDR VARCHAR(255), LIT_REF VARCHAR(255), AGE VARCHAR(255), AGE_COD VARCHAR(255),
        AGE_GRP VARCHAR(255), SEX VARCHAR(255), GNDR_COD VARCHAR(255), E_SUB VARCHAR(255), WT VARCHAR(255),
        WT_COD VARCHAR(255), REPT_DT VARCHAR(255), OCCP_COD VARCHAR(255), DEATH_DT VARCHAR(255), TO_MFR VARCHAR(255),
        CONFID VARCHAR(255), REPORTER_COUNTRY VARCHAR(255), OCCR_COUNTRY VARCHAR(255))
        """)
        cursor.execute("""
        create table drug (ISR VARCHAR(255), PRIMARYID VARCHAR(255), CASEID VARCHAR(255), DRUG_SEQ VARCHAR(255),
        ROLE_COD VARCHAR(255), DRUGNAME VARCHAR(255), PROD_AI VARCHAR(255), VAL_VBM VARCHAR(255), ROUTE VARCHAR(255),
        DOSE_VBM VARCHAR(255), CUM_DOSE_CHR VARCHAR(255), CUM_DOSE_UNIT VARCHAR(255), DECHAL VARCHAR(255),
        RECHAL VARCHAR(255), LOT_NUM VARCHAR(255), EXP_DT VARCHAR(255), NDA_NUM VARCHAR(255), DOSE_AMT VARCHAR(255),
        DOSE_UNIT VARCHAR(255), DOSE_FORM VARCHAR(255), DOSE_FREQ VARCHAR(255))
        """)
        cursor.execute("""
        create table react (ISR VARCHAR(255), PRIMARYID VARCHAR(255), CASEID VARCHAR(255), PT VARCHAR(255),
        DRUG_REC_ACT VARCHAR(255))
        """)
        cursor.execute("""
        create table outcome (ISR VARCHAR(255), PRIMARYID VARCHAR(255), CASEID VARCHAR(255), OUTC_COD VARCHAR(255))
        """)
        cursor.execute("""
        create table source (ISR VARCHAR(255), PRIMARYID VARCHAR(255), CASEID VARCHAR(255), RPSR_COD VARCHAR(255))
        """)
        cursor.execute("""
        create table therapy (ISR VARCHAR(255), PRIMARYID VARCHAR(255), CASEID VARCHAR(255), DRUG_SEQ VARCHAR(255),
        START_DT VARCHAR(255), END_DT VARCHAR(255), DUR VARCHAR(255), DUR_COD VARCHAR(255))
        """)
        cursor.execute("""
        create table indication (ISR VARCHAR(255), PRIMARYID VARCHAR(255), CASEID VARCHAR(255),
        DRUG_SEQ VARCHAR(255), INDI_DRUG_SEQ VARCHAR(255), INDI_PT VARCHAR(255))
        """)
        cursor.execute("""alter table drug add index PRIMARYID (PRIMARYID)""")
        cursor.execute("""alter table drug add index role_cod (role_cod(4))""")
        cursor.execute("""alter table drug add index DRUGNAME (DRUGNAME(10))""")
        cursor.execute("""alter table drug add index PROD_AI (PROD_AI(10))""")
        cursor.execute("""alter table drug add index CASEID (CASEID)""")
        cursor.execute("""alter table indication add index PRIMARYID (PRIMARYID)""")
        cursor.execute("""alter table indication add index indi_drug_seq (indi_drug_seq)""")
        cursor.execute("""alter table demo add index CASEID (CASEID)""")
        cursor.execute('set global max_allowed_packet=1073741824')
        self.mydb.commit()

        # https://stackoverflow.com/questions/2108824/mysql-incorrect-string-value-error-when-save-unicode-string-in-django
        dbname = "faers"
        cursor = self.mydb.cursor()
        cursor.execute("ALTER DATABASE `%s` CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'" % dbname)
        sql = "SELECT DISTINCT(table_name) FROM information_schema.columns WHERE table_schema = '%s'" % dbname
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            sql = "ALTER TABLE `%s` convert to character set DEFAULT COLLATE DEFAULT" % (row[0])
            cursor.execute(sql)
        self.mydb.commit()

    def writeEntry(self, table_name, field_names, list_of_fields):
        """
        writeEntry() takes a table_name and list of list of fields and inserts them.
        """
        fs = ' (' + ','.join(field_names) + ')'
        qs = ['%s'] * len(list_of_fields[0])
        stm = 'INSERT INTO ' + table_name + fs + ' VALUES(' + ', '.join(qs) + ')'
        # print(stm)
        cursor = self.mydb.cursor()
        cursor.executemany(stm, list_of_fields)
        self.mydb.commit()

    def closeDB(self):
        """
        closeDB() commits and closes the Db connection.
        """
        self.mydb.commit()
        self.mydb.close()


# The FDA changes their data structure and ordering between releases.
class DBfields:
    def __init__(self, year, quarter):
        self.yearq = year + 0.1 * quarter
        self.trans = {
            'THER': ['therapy', self.therapy_fields],
            'RPSR': ['source', self.source_fields],
            'REAC': ['react', self.react_fields],
            'OUTC': ['outcome', self.outcome_fields],
            'INDI': ['indication', self.indication_fields],
            'DRUG': ['drug', self.drug_fields],
            'DEMO': ['demo', self.demo_fields],
        }

    def translate(self, first_four):
        table_name = self.trans[first_four][0]
        table_fields = self.trans[first_four][1]()
        return {'table_name': table_name, 'table_fields': table_fields}

    def therapy_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'DRUG_SEQ', 'START_DT', 'END_DT', 'DUR', 'DUR_COD']
        else:
            return ['PRIMARYID', 'CASEID', 'DRUG_SEQ', 'START_DT', 'END_DT', 'DUR', 'DUR_COD']

    def source_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'RPSR_COD']
        else:
            return ['PRIMARYID', 'CASEID', 'RPSR_COD']

    def react_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'PT']
        elif self.yearq < 2014.3:
            return ['PRIMARYID', 'CASEID', 'PT']
        else:
            return ['PRIMARYID', 'CASEID', 'PT', 'DRUG_REC_ACT']

    def outcome_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'OUTC_COD']
        else:
            return ['PRIMARYID', 'CASEID', 'OUTC_COD']

    def indication_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'DRUG_SEQ', 'INDI_PT']
        else:
            return ['PRIMARYID', 'CASEID', 'INDI_DRUG_SEQ', 'INDI_PT']

    def drug_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'DRUG_SEQ', 'ROLE_COD', 'DRUGNAME', 'VAL_VBM', 'ROUTE', 'DOSE_VBM', 'DECHAL', 'RECHAL',
                    'LOT_NUM', 'EXP_DT', 'NDA_NUM']
        elif self.yearq < 2014.3:
            return ['PRIMARYID', 'CASEID', 'DRUG_SEQ', 'ROLE_COD', 'DRUGNAME', 'VAL_VBM', 'ROUTE', 'DOSE_VBM',
                    'CUM_DOSE_CHR', 'CUM_DOSE_UNIT',
                    'DECHAL', 'RECHAL', 'LOT_NUM', 'EXP_DT', 'NDA_NUM', 'DOSE_AMT', 'DOSE_UNIT', 'DOSE_FORM',
                    'DOSE_FREQ']
        else:
            return ['PRIMARYID', 'CASEID', 'DRUG_SEQ', 'ROLE_COD', 'DRUGNAME', 'PROD_AI', 'VAL_VBM', 'ROUTE',
                    'DOSE_VBM', 'CUM_DOSE_CHR', 'CUM_DOSE_UNIT',
                    'DECHAL', 'RECHAL', 'LOT_NUM', 'EXP_DT', 'NDA_NUM', 'DOSE_AMT', 'DOSE_UNIT', 'DOSE_FORM',
                    'DOSE_FREQ']

    def demo_fields(self):
        if self.yearq < 2012.4:
            return ['ISR', 'CASE_NUM', 'I_F_COD', 'FOLL_SEQ', 'IMAGE', 'EVENT_DT', 'MFR_DT', 'FDA_DT', 'REPT_COD',
                    'MFR_NUM', 'MFR_SNDR', 'AGE', 'AGE_COD',
                    'GNDR_COD', 'E_SUB', 'WT', 'WT_COD', 'REPT_DT', 'OCCP_COD', 'DEATH_DT', 'TO_MFR', 'CONFID',
                    'REPORTER_COUNTRY']
        elif self.yearq < 2014.3:
            return ['PRIMARYID', 'CASEID', 'CASEVERSION', 'I_F_COD', 'EVENT_DT', 'MFR_DT', 'INIT_FDA_DT', 'FDA_DT',
                    'REPT_COD', 'MFR_NUM', 'MFR_SNDR',
                    'AGE', 'AGE_COD', 'GNDR_COD', 'E_SUB', 'WT', 'WT_COD', 'REPT_DT', 'TO_MFR', 'OCCP_COD',
                    'REPORTER_COUNTRY', 'OCCR_COUNTRY']
        else:
            return ['PRIMARYID', 'CASEID', 'CASEVERSION', 'I_F_COD', 'EVENT_DT', 'MFR_DT', 'INIT_FDA_DT', 'FDA_DT',
                    'REPT_COD', 'AUTH_NUM', 'MFR_NUM',
                    'MFR_SNDR', 'LIT_REF', 'AGE', 'AGE_COD', 'AGE_GRP', 'SEX', 'E_SUB', 'WT', 'WT_COD', 'REPT_DT',
                    'TO_MFR', 'OCCP_COD', 'REPORTER_COUNTRY', 'OCCR_COUNTRY']


db = DB()

tbl_count = {}

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
def sort_files(a, b):
    if a[2] != b[2]:
        return a[2] - b[2]
    return a[3] - b[3]


valid_files.sort(key=functools.cmp_to_key(lambda x, y: sort_files(x, y)))


def pop_newlines(fields, req_fields):
    while len(fields) > req_fields and (fields[-1] == "\r\n" or fields[-1] == "" or fields[-1] == "\n"):
        fields.pop()


# Verify every field in the files to make sure they add up.  These records are
# likely manually entered and need to be cleaned.
for zip_files in valid_files:
    list_of_fields = []
    # Unpack list of info
    zip_filename = zip_files[0]
    filename = zip_files[1]
    year = zip_files[2]
    quarter = zip_files[3]

    zip_name = ascii_file_re.search(zip_files[1]).group(1)
    if zip_name[:4] == 'STAT':
        continue  # STAT is not $ delimited and likely wrong after scrubbing

    trans = DBfields(year, quarter).translate(zip_name[:4].upper())
    table_name = trans['table_name']

    print(zip_filename + ' ' + filename + ' (' + trans['table_name'] + ')')
    f = zipfile.ZipFile(zip_filename, 'r')
    h = f.open(filename, 'rU')

    lines = h.readlines()
    total_lines = len(lines)
    req_fields = len(trans['table_fields'])
    fields_obj = DBfields(year, quarter)
    i = 0
    while i < total_lines:
        # Skip the first line since it is only headers
        if i == 0:
            i += 1
            continue

        l = lines[i].decode('utf-8').strip()
        fields = l.split('$')

        # Try to concat the next lines if the field count doesn't add up
        extra_lines = 0
        while len(fields) < req_fields and i + 1 + extra_lines < total_lines:
            extra_lines += 1
            l += lines[i + extra_lines]
            fields = l.split('$')
            # Check if we went over the field count and give up
            pop_newlines(fields, req_fields)
            if len(fields) > req_fields:
                print("\t", zip_files[1], i + 1, len(fields), req_fields)
                fields = lines[i].split('$')
                extra_lines = 0
                break

        # Some files have extra blank fields
        pop_newlines(fields, req_fields)

        field_count = len(fields)
        if field_count == req_fields:
            # TODO remove all newline characters from entries
            try:
                if table_name not in tbl_count:
                    tbl_count[table_name] = {'records': 0, 'files': 0}
                tbl_count[table_name]['records'] += 1
                fields = [f.strip()[:255] for f in fields]
                fields = [f if f else None for f in fields]
                list_of_fields.append(fields)
            except Exception as e:
                print(l)
                print(fields)
                raise Exception(e)
        else:
            # FDA probably forgot to escape a $
            print("\t", trans['table_name'], ' - skipping line ', i + 1, year, quarter, len(fields), req_fields)
            print("\t\t", fields)

        i += 1 + extra_lines
    print("importing")
    db.writeEntry(table_name, trans['table_fields'], list_of_fields)
    print('done')
    tbl_count[table_name]['files'] += 1
    db.mydb.commit()

db.closeDB()
print(tbl_count)
