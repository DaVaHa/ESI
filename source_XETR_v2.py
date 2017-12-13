import requests
from bs4 import BeautifulSoup
import re
import numpy as np


#load main page into BeautifulSoup
url = 'https://www.bundesanzeiger.de/ebanzwww/wexsservlet?page.navid=to_nlp_start'
r = requests.get(url)
html = r.text
soup = BeautifulSoup(html, 'lxml')

body = soup.find('tbody')

hrefs = []
for tag in body.find_all('a'):
    hrefs.append(tag.get('href'))

print(len(hrefs))
