#!/usr/bin/env python

from SECCrawler import SECCrawler
from threading import Lock

class Trigger(SECCrawler):
    def __init__(self):
        super(Trigger, self).__init__()
        self.process_lock = Lock()

    def process_document(self, doc):
        self.process_lock.acquire()
        print 'GET', doc.status, doc.url
        self.process_lock.release()

search_link = "https://www.sec.gov/cgi-bin/srch-edgar?text="
file_types = ["10-K","10-Q","8-K"]
start_year = "2010"
end_year = "2017"

search_text  = ""
for type in file_types:
    search_text = search_text + "type%3D"+type+"%20or%20"

search_text = search_text[:-5]
search_text = search_text + "&first="+start_year+"&last=2017"
print(search_text)

t = Trigger()
t.set_search_link(search_link+search_text)
t.crawl_search_result()
