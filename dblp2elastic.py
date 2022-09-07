'''
conda create -n es python
conda activate es
pip install elastic-enterprise-search
python dblp2elastic.py <private api key>
'''

import gzip
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET

from elastic_enterprise_search import AppSearch
from elastic_enterprise_search.exceptions import BadRequestError, NotFoundError


engine = 'dblp'
batch_size = 100
booktitle_volume = re.compile(r'(.*) \(\d+\)')


if __name__ == '__main__':
    app_search = AppSearch('http://localhost:3002', bearer_auth=sys.argv[1], request_timeout=30)

    try:
        app_search.delete_engine(engine_name=engine)
    except NotFoundError:
        pass

    created = False
    while not created:
        try:
            app_search.create_engine(engine_name=engine)
            created = True
        except BadRequestError:
            time.sleep(1)

    with urllib.request.urlopen('https://github.com/emeryberger/CSrankings/raw/gh-pages/dblp.xml.gz') as f, \
        gzip.open(f, 'rt', encoding='utf-8') as g:
        root = ET.parse(g).getroot()
    papers = []
    for p in root:
        paper = {}
        for e in p:
            if e.tag in ('author', 'cdrom', 'cite', 'editor'):
                paper.setdefault(e.tag, []).append(e.text)
            elif e.tag == 'booktitle':
                match = booktitle_volume.fullmatch(e.text)
                if match:
                    paper[e.tag] = match[1]
                else:
                    paper[e.tag] = e.text
            elif e.tag == 'year':
                paper[e.tag] = int(e.text)
            else:
                paper[e.tag] = e.text
        papers.append(paper)
        if len(papers) >= batch_size:
            app_search.index_documents(engine_name=engine, documents=papers)
            papers.clear()
    if papers:
        app_search.index_documents(engine_name=engine, documents=papers)
