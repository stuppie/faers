from bs4 import BeautifulSoup
import os
import requests
from urllib.request import urlretrieve
import re

from faers.settings import FAERS_DATA_PATH

URL = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"


def download_new_files():
    if not os.path.isdir(FAERS_DATA_PATH):
        os.makedirs(FAERS_DATA_PATH)
    existing_file_names = set(os.listdir(FAERS_DATA_PATH))

    bs = BeautifulSoup(requests.get(URL).text, "lxml")
    urls = [x['href'] for x in bs.find_all("a") if "ASCII" in x.text]
    url_year = {url: int(re.findall('aers_ascii_([\d]+)q\d.zip', url)[0]) for url in urls}

    # only get from 2013 and after
    urls = [url for url in urls if url_year[url] >= 2013]

    new_file_names = set([os.path.split(x)[-1] for x in urls]) - existing_file_names
    urls = [x for x in urls if os.path.split(x)[-1] in new_file_names]

    print("Download {} new file{}".format(len(urls), "s" if len(urls) != 1 else ""))
    for url in urls:
        file_name = os.path.split(url)[-1]
        print("Downloading: {}".format(file_name))
        urlretrieve(url, os.path.join(FAERS_DATA_PATH, file_name))

    return new_file_names