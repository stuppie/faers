import os

from faers.settings import MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_DB
from faers import downloader, dedupe, parser, normalize_indications, normalize_drugs, get_indications
import mysql.connector


class Pipeline:
    def __init__(self, first_time=True):
        # if first_time, it'll create the initial tables. otherwise, will assume tables exist and update using new files
        self.mydb = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, database=MYSQL_DB)
        self.first_time = first_time

    def download_new_files(self):
        self.new_files = downloader.download_new_files()

    def parse_and_load_data(self):
        valid_files = parser.get_valid_files()
        if self.first_time:
            parser.create_db(MYSQL_DB)
            parser.create_tables(valid_files)
        else:
            # only update new files
            valid_files = [f for f in valid_files if os.path.split(f[0])[1] in self.new_files]
        parser.import_data(valid_files)

    def dedupe(self):
        cursor = self.mydb.cursor()
        cursor.execute("DROP DATABASE IF EXISTS drug_latest")
        cursor.execute("DROP DATABASE IF EXISTS indication_latest")
        self.mydb.commit()

        dedupe.make_dedupe_tables()

    def run(self):
        self.download_new_files()
        self.parse_and_load_data()
        self.dedupe()
        normalize_indications.run()
        normalize_drugs.run()
        get_indications.run()
