''''
This script will scrape all the German Short Interest from BundesAnzeiger.de.
'''

import requests
from bs4 import BeautifulSoup
import re
import numpy as np
import time

# start time
startTime = time.time()


#load main page into BeautifulSoup
url = 'https://www.bundesanzeiger.de/ebanzwww/wexsservlet?page.navid=to_nlp_start'
r = requests.get(url)
html = r.text
soup = BeautifulSoup(html, 'lxml')

##def DetermineNextPage(url, prev_url=None):
##
##    # break if next_page == previous url
##    if prev_url == url:
##        print("Found same page:\n{}".format(url))
##        return None
##    
##    # load bs object
##    r = requests.get(url)
##    html = r.text
##    soup = BeautifulSoup(html, 'lxml')
##    
##    # get all pages with info
##    nav = soup.find_all('ul', class_='page_navigation')  #2 results
##    hrefs = []
##    for tag in nav[0].find_all('a'):
##        hrefs.append(tag.get('href'))   # all links for pages
##
##    pages = []
##    for href in hrefs:
##        pages.append(href[re.search('currentpage=', href).start()+len('currentpage='):])
##
##    # determine next page
##    p = [p for p in pages if pages.count(p) > 1]
##    print(p)
##    next_page = list(np.unique(p))[-1]
##    print(next_page)
##    next_page = 'https://www.bundesanzeiger.de' + [l for l in hrefs if l.endswith(next_page)][0]
##    print(next_page)
##       
##    # run next page
##    DetermineNextPage(next_page, prev_url=url)

####DetermineNextPage(url)

# get total number of pages
p = soup.find_all('li', class_="first entry_count")[0].text
page_nrs = int([t for t in re.findall('\d+', p) if int(t) < 100][0])
print(page_nrs)

# get link without page number
nav = soup.find_all('ul', class_='page_navigation')
hrefs = []
for tag in nav[0].find_all('a'):
    hrefs.append(tag.get('href'))
print(len(hrefs))

pages = []
for href in hrefs:
    if re.search('currentpage=', href):
        pages.append(href)
print(pages)

uniques = []
for page in pages:
    uniques.append(page[:re.search('currentpage=', page).start()+len('currentpage=')])

uniques = list(np.unique(uniques))
print(uniques)

# loop over all pages
for i in range(page_nrs):
    print('https://www.bundesanzeiger.de{}{}'.format(uniques[0],i+1))







# end time
endTime = time.time()
print("Script ran for {} seconds.".format(round(endTime-startTime,2)))




