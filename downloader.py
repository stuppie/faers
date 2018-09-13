from bs4 import BeautifulSoup
import os
import requests
from urllib.request import urlretrieve

DATA_PATH = "data"
URL = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"

if not os.path.isdir(DATA_PATH):
    os.makedirs(DATA_PATH)
existing_file_names = set(os.listdir(DATA_PATH))

bs = BeautifulSoup(requests.get(URL).text, "lxml")
urls = [x['href'] for x in bs.find_all("a") if "ASCII" in x.text]
new_file_names = set([os.path.split(x)[-1] for x in urls]) - existing_file_names
urls = [x for x in urls if os.path.split(x)[-1] in new_file_names]

print("Download {} new file{}".format(len(urls), "s" if len(urls) != 1 else ""))
for url in urls:
    file_name = os.path.split(url)[-1]
    print("Downloading: {}".format(file_name))
    urlretrieve(url, os.path.join(DATA_PATH, file_name))