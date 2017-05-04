import requests
import sqlite3
import os
import sys
import json
import time
import csv
import re


"""
Basic notes:

Process steps:
- Organize list of titles to download
- Get extracted content and intro content for them and categories
- if categories are not "XXXX video games" log bad URL name
- Log the revision number for each page

"""

DEFAULT_RESOURCE_PATH = os.path.expanduser("~/Library/Application Support/wiki_corpus")
TITLES_FILE = os.path.expanduser("~/Library/Application Support/wiki_corpus/titles.tsv")

if not os.path.exists(TITLES_FILE):
    print "Titles file not found at {}".format(TITLES_FILE)
    print "Exiting"
    sys.exit()

if not os.path.isdir(DEFAULT_RESOURCE_PATH):
    os.mkdir(DEFAULT_RESOURCE_PATH)

conn = sqlite3.connect(os.path.join(DEFAULT_RESOURCE_PATH, "wiki.db"))

c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type=? AND name=?", ('table', 'revision_index'))

if not c.fetchone():
    c.execute("create table revision_index (rid text, url text, is_stub boolean)")
    c.execute("create table false_index (url text)")

conn.commit()

"""
https://en.wikipedia.org/w/api.php?action=query&titles=The_Oregon_Trail_(video_game)&prop=extracts|revisions&rvprop=ids&explaintext&format=json
"""

header = "https://en.wikipedia.org/w/api.php"
params = {
    'action': 'query',
    'prop': 'extracts',
    'explaintext': '',
    'format': 'json'
    }

intro_params = {
    'action': 'query',
    'prop': 'extracts|revisions|categories',
    'rvprop': 'ids',
    'exintro': '',
    'format': 'json'
    }

class Result:

    def __init__(self):
        self.results = {}
        self.count = 424

    def increase_count(self):
        self.count = self.count + 1

    def clear_results(self):
        self.results = {}

    def __getitem__(self, key):
        return self.results[key]

    def __setitem__(self, key, value):
        self.results[key] = value

    def __iter__(self):
        for key in self.results:
            yield key

result_obj = Result()
batch_size = 50
total_full_batches = 0
total_remainder = 0

# Get total number of records
total_titles = 0
with open(TITLES_FILE, 'rb') as t:
    for row in csv.reader(t, delimiter='\t'):
        total_titles = total_titles + 1

print "{} Total titles found in titles.tsv".format(total_titles)

total_full_batches = total_titles // batch_size
total_remainder = total_titles % batch_size

print "{} total batches\n{} total_remainder".format(total_full_batches, total_remainder)

def process_intro(intro_object):
    """ 
Basic structure of intro_object:

batchcomplete -> flag for end of request if multiple
query -> contents of query
    normalized -> page range for page range queries (not used)
    pages -> collection of returned pages (usually one per call)
    pages[pageid] -> content we are after
        extract -> intro text blob
        ns -> no clue :)
        pageid -> id of current page
        revisions -> list of page revision dicts 
            [{'revid': current revision, 'parent': previous revision}] 
        title -> page title in plain text
        categories -> list of categorie dicts 
            [{'ns': integer, 'title': 'Category:(category name)}] 
"""
    page_dict = intro_object['pages']
    for pageid in page_dict.keys():
        page_info = page_dict[pageid]

        if pageid not in result_obj:
            result_obj[pageid] = {'pageid': pageid}

        if 'extract' in page_info:
            intro_text_html = page_info['extract'].replace('\n',' ').replace('\t',' ')
            result_obj[pageid]['intro_text_html'] = intro_text_html

        if 'revisions' in page_info:
            result_obj[pageid]['revision_id'] = str(page_info['revisions'][0]['revid'])

        if 'title' in page_info:
            result_obj[pageid]['title'] = page_info['title']

        if 'categories' in page_info:
            categories =  "|".join([ cat['title'].split(':')[1] for cat in page_info['categories']])
            result_obj[pageid]['categories'] = categories
            match = re.search("[0-9]{4} video games", categories)
            if match:
                result_obj[pageid]['year'] = match.group(0)[:4]
            

def process_content(content_object):
    """ 
Basic structure of content_object is identical to intro_object
with the exception that query->pages->pageid->extract returns
the full page content instead of the intro text
"""
    page_dict = content_object['pages']
    for pageid in page_dict.keys():
        page_info = page_dict[pageid]
        if pageid in result_obj:
            if 'extract' in page_info:
                full_text = page_info['extract'].replace('\n',' ').replace('\t',' ')
                result_obj[pageid]['full_text'] = full_text
        else:
            print "WARNING: Page {} with title {} is probably not a valid game page.".format(pageid, page_info['title'])


def query(params):
    last_continue = {'continue': ''}
    while True:
        p = params.copy()
        p.update(last_continue)

        result = requests.post(header, params=p).json()

        if 'error' in result:
            raise Error(result['error'])
        if 'warnings' in result:
            print result['warnings']
        if 'query' in result:
            yield result['query']
        if 'continue' not in result:
            break
        last_continue = result['continue']

def write_and_clear_results(count):
    file_name = os.path.join(DEFAULT_RESOURCE_PATH, 'corpus_{}.tsv'.format(count))
    with open(file_name, 'wb') as tsv_file:
        field_names = ['title', 'pageid','revision_id', 'year', 'intro_text_html', 'full_text', 'categories']
        writer = csv.DictWriter(tsv_file, field_names, delimiter='\t')
        writer.writeheader()
        # Doing really stupid page filtering here
        # might make it more impressive later
        for pageid in result_obj:
            if 'full_text' not in result_obj[pageid] or result_obj[pageid]['full_text'] == '':
                print "Excluding page '{}' because it has no extract".format(result_obj[pageid]['title'].encode('utf8'))
                c.execute("insert into false_index (url) values (?)", (result_obj[pageid]['title'],))
                conn.commit()
                continue
            if re.search("Redirect", result_obj[pageid]['categories']):
                print "Excluding page '{}' because it is probably a redirect".format(result_obj[pageid]['title'].encode('utf8'))
                c.execute("insert into false_index (url) values (?)", (result_obj[pageid]['title'],))
                conn.commit()
                continue

            writer.writerow({k: v.encode('utf8') for k, v in result_obj[pageid].items()})

# Iterate over records in batchs of 'batch_size' and store results in
# a contents.tsv per batch
with open(TITLES_FILE, 'rb') as t:
    t_reader = csv.reader(t, delimiter='\t')
    titles_to_scrape = []
    processed_batches = 0
    for i, row in enumerate(t_reader):
        titles_to_scrape.append(row[0])
        if (i + 1) % batch_size == 0 and i != 0 or \
           total_remainder != 0 and \
                              ((i + 1) - processed_batches * batch_size) == total_remainder and \
                              processed_batches == total_full_batches:
            print "Processing batch: {}, from: {} to: {}".format(result_obj.count, titles_to_scrape[0], titles_to_scrape[-1])
            titles = "|".join(titles_to_scrape)
            # These are separate queries because the api cannot return
            # an intro summary and full content at the same time
            intro_req = intro_params.copy()
            intro_req['titles'] = titles
            for result in query(intro_req):
                process_intro(result)

            content_req = params.copy()
            content_req['titles'] = titles
            for result in query(content_req):
                process_content(result)

            write_and_clear_results(result_obj.count)
            result_obj.increase_count()
            result_obj.clear_results()
            time.sleep(5)

            processed_batches = processed_batches + 1
            titles_to_scrape = []
            

