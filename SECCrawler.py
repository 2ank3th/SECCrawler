from __future__ import print_function

import os
import re
import sys

from threading import Thread, Lock

if sys.version_info < (3, 0):
    import httplib

    from urllib import quote
else:
    import http.client as httplib
    from urllib.parse import quote

from bs4 import BeautifulSoup

class Document(object):
    def __init__(self, res, url):
        self.url = url
        self.query = '' if '?' not in url else url.split('?')[-1]
        self.status = res.status
        self.text = res.read()
        self.headers = dict(res.getheaders())

        if sys.version_info >= (3, 0):
            self.text = self.text.decode()


class SECCrawler(object):
    F_ANY, F_SAME_DOMAIN, F_SAME_HOST, F_SAME_PATH = list(range(4))
    SEARCH_RESULT, CONTENTS, DETAILS = list(range(3))
    key_words = ["credit agreement","credit and guarantee agreement","security and collateral agreement","intercreditor agreement","bridge credit agreement","asset based lending agreement","debtor in possession agreement"]

    def __init__(self):
        self.host = None
        self.visited = {}
        self.targets = set()
        self.threads = []
        self.concurrency = 0
        self.max_outstanding = 8
        self.max_depth = 0
        self.include_hashtag = False

        self.follow_mode = self.F_SAME_HOST
        self.content_type_filter = '(text/html)'
        self.url_filters = [(self.CONTENTS,"-index.htm"),(self.DETAILS, "/Archives/edgar/data/")]
        self.prefix_filter = '^(#|javascript:|mailto:)'

        self.targets_lock = Lock()
        self.concurrency_lock = Lock()
        self.search_link = None
        self.crawled = 0
        self.start = 1
        self.count = 80

    def set_search_link(self,link):
        self.search_link = link

    def set_content_type_filter(self, cf):
        self.content_type_filter = '(%s)' % ('|'.join(cf))

    def add_url_filter(self, uf):
        self.url_filters.append(uf)

    def set_follow_mode(self, mode):
        if mode > 5:
            raise RuntimeError('invalid follow mode.')
        self.follow_mode = mode

    def set_concurrency_level(self, level):
        self.max_outstanding = level

    def set_max_depth(self, max_depth):
        self.max_depth = max_depth

    def set_include_hashtag(self, include):
        self.include_hashtag = include

    def process_documents(self,parent_url, doc, link_type):
        # Make unique list
        links = re.findall(
            '''href\s*=\s*['"]\s*([^'"]+)['"]''', doc.text, re.S)
        links = list(set(links))

        if link_type == self.SEARCH_RESULT:
            for link in links:
                if link[0] == "/":
                    link =  "https://www.sec.gov" + link
                rlink = self._follow_link(doc.url, link.strip(),self.CONTENTS)
                if rlink:
                    self._add_target(rlink, self.CONTENTS)

        elif link_type == self.CONTENTS:
            soup = BeautifulSoup(doc.text,"lxml")

            table = soup.find("table", attrs={"class": "tableFile"})

            datasets = []
            headings = [th.get_text() for th in table.find("tr").find_all("th")]

            for row in table.find_all("tr")[1:]:
                dataset = zip(headings, (td.get_text() for td in row.find_all("td")))

                for td in row.find_all("td"):
                    for a in td.find_all('a', href=True):
                        link = a['href']
                datasets.append((dataset,link))

            for row,link in datasets:
                if "EXHIBIT 10" in row[1][1]:
                    rlink = self._follow_link(doc.url, link.strip(),self.DETAILS)
                    if rlink:
                        self._add_target(rlink, self.DETAILS)

        elif link_type == self.DETAILS:
            lower_text = doc.text.lower()
            if any( key in lower_text for key in self.key_words ):
                filename = doc.url.split("/")[-1]
                print(filename)
                text_file = open(filename, "w")
                text_file.write(doc.text)
                text_file.close()

        # to do stuff with url depth use self._calc_depth(doc.url)l

    def search_next_page(self):
        url = self.search_link
        url = url + "&start=" + str(self.start) + "&count=" + str(self.count)
        self.targets.add((url, self.SEARCH_RESULT))
        return url

    def crawl_search_result(self):

        url = self.search_next_page()

        rx = re.match('(https?://)([^/]+)([^\?]*)(\?.*)?', url)
        self.proto = rx.group(1)
        self.host = rx.group(2)
        self.path = rx.group(3)
        self.dir_path = os.path.dirname(self.path)
        self.query = rx.group(4)

        self._spawn_new_worker()

        while self.threads:
            try:
                for t in self.threads:
                    if t.isAlive():
                        t.join(1)
                    else:
                        self.threads.remove(t)
            except KeyboardInterrupt:
                sys.exit(1)

    def _url_domain(self, host):
        parts = host.split('.')
        if len(parts) <= 2:
            return host
        elif re.match('^[0-9]+(?:\.[0-9]+){3}$', host):  # IP
            return host
        else:
            return '.'.join(parts[1:])

    def _follow_link(self, url, link,link_type):
        # Skip prefix
        if re.search(self.prefix_filter, link):
            return None


        filters = [filter for _link_type,filter in self.url_filters if _link_type == link_type]
        # Filter url
        for f in filters:
            if not re.search(f, link):
                return None

        if not self.include_hashtag:
            link = re.sub(r'(%23|#).*$', '', link)

        rx = re.match('(https?://)([^/:]+)(:[0-9]+)?([^\?]*)(\?.*)?', url)
        url_proto = rx.group(1)
        url_host = rx.group(2)
        url_port = rx.group(3) if rx.group(3) else ''
        url_path = rx.group(4) if len(rx.group(4)) > 0 else '/'
        url_dir_path = os.path.dirname(url_path)

        rx = re.match('((https?://)([^/:]+)(:[0-9]+)?)?([^\?]*)(\?.*)?', link)
        link_full_url = rx.group(1) is not None
        link_proto = rx.group(2) if rx.group(2) else url_proto
        link_host = rx.group(3) if rx.group(3) else url_host
        link_port = rx.group(4) if rx.group(4) else url_port
        link_path = quote(rx.group(5), '/%') if rx.group(5) else url_path
        link_query = quote(rx.group(6), '?=&%') if rx.group(6) else ''
        link_dir_path = os.path.dirname(link_path)

        if not link_full_url and not link.startswith('/'):
            link_path = os.path.normpath(os.path.join(url_dir_path, link_path))

        link_url = link_proto + link_host + link_port + link_path + link_query

        if self.follow_mode == self.F_ANY:
            return link_url
        elif self.follow_mode == self.F_SAME_DOMAIN:
            return link_url if self._url_domain(self.host) == \
                self._url_domain(link_host) else None
        elif self.follow_mode == self.F_SAME_HOST:
            return link_url if self.host == link_host else None
        elif self.follow_mode == self.F_SAME_PATH:
            if self.host == link_host and \
                    link_dir_path.startswith(self.dir_path):
                return link_url
            else:
                return None

    def _calc_depth(self, url):
        # calculate url depth
        return len(url.replace('https', 'http').replace(
            self.root_url, '').rstrip('/').split('/')) - 1

    def _add_target(self, target,link_type):
        if not target:
            return

        if self.max_depth and self._calc_depth(target) > self.max_depth:
            return

        with self.targets_lock:
          if target in self.visited:
              return
          self.targets.add((target,link_type))

    def _spawn_new_worker(self):
        with self.concurrency_lock:
          self.concurrency += 1
          t = Thread(target=self._worker, args=(self.concurrency,))
          t.daemon = True
          self.threads.append(t)
          t.start()

    def _worker(self, sid):
        while self.targets:
            try:
                with self.targets_lock:
                  url,link_type = self.targets.pop()
                  self.visited[url] = True

                rx = re.match('(https?)://([^/]+)(.*)', url)
                protocol = rx.group(1)
                host = rx.group(2)
                path = rx.group(3)

                if protocol == 'http':
                    conn = httplib.HTTPConnection(host, timeout=10)
                else:
                    conn = httplib.HTTPSConnection(host, timeout=10)

                conn.request('GET', path)
                res = conn.getresponse()

                if res.status == 404:
                    continue

                if res.status == 301 or res.status == 302:
                    rlink = self._follow_link(url, res.getheader('location'))
                    self._add_target(rlink,link_type)
                    continue

                # Check content type
                try:
                    if not re.search(
                        self.content_type_filter,
                            res.getheader('Content-Type')):
                        continue
                except TypeError:  # getheader result is None
                    continue

                doc = Document(res, url)
                self.process_documents(url,doc, link_type)

                if self.concurrency < self.max_outstanding:
                    self._spawn_new_worker()
            except KeyError:
                # Pop from an empty set
                break
            except (httplib.HTTPException, EnvironmentError):
                with self.targets_lock:
                  self._add_target(url,link_type)

        with self.concurrency_lock:
          self.concurrency -= 1

        if self.concurrency == 0:
            self.start += 80
            print(self.start)
            self.search_next_page()
            self._spawn_new_worker()