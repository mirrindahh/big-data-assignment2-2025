import string
import sys
from collections import Counter
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

# Keyspace
session.execute('''
CREATE KEYSPACE IF NOT EXISTS bm25_index
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
''')
session.set_keyspace('bm25_index')

session.execute('''\
CREATE TABLE IF NOT EXISTS docs (
    doc_id TEXT PRIMARY KEY,
    doc_title TEXT,
    dl INT
)''')
session.execute('''\
CREATE TABLE IF NOT EXISTS tfs (
    doc_id TEXT,
    term TEXT,
    tf INT,
    PRIMARY KEY (doc_id, term)
)''')

alphabet = set(string.ascii_lowercase)
for line in sys.stdin:
    doc_id, doc_title, doc_text = line.split('\t')

    # Compute tf
    counter = Counter()
    for term in (doc_title + doc_text).split():
        term = ''.join(c for c in term.lower() if c in alphabet)
        if len(term) != 0:
            counter[term] += 1

    # Docs
    session.execute('INSERT INTO docs (doc_id, doc_title, dl) VALUES (%s, %s, %s)', (doc_id, doc_title, sum(counter.values())))

    # tfs
    for term, tf in counter.most_common():
        session.execute('INSERT INTO tfs (doc_id, term, tf) VALUES (%s, %s, %s)', (doc_id, term, tf))
        # This is needed to calculate dfs
        print(term, doc_id, sep="\t")
