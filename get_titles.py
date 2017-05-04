import requests
from bs4 import BeautifulSoup
import time

dump_file = "titles.txt"
post_data = {
    'addcat': 'Add',
    'curonly': '1',
    'wpDownload': '1',
    'title': 'Special:Export'
}

def write_cat_page(cat):
    post_data['catname'] = cat
    cat_page = requests.post("https://en.wikipedia.org/wiki/Special:Export", post_data).text
    print "getting cat {}".format(cat)
    parsed_page = BeautifulSoup(cat_page, 'html.parser')
    cats = parsed_page.find(id='mw-input-pages').contents[0].string
    if cats:
        cats = cats.split('\n')
        with open(dump_file, 'a') as f:
            for cat in cats:
                cat += '\t{}\n'.format(year)
                f.write(cat.encode('utf8'))

for year in range(1947, 2017):
    write_cat_page("{} video games".format(year))

write_cat_page("Upcoming video games scheduled for 2017")
