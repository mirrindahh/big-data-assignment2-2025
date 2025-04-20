import sys
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect('bm25_index')

session.execute('''\
CREATE TABLE IF NOT EXISTS dfs (
    term TEXT PRIMARY KEY,
    df INT
)''')


df = 0
current_term = None

for line in sys.stdin:
    # doc_id are already unique, since we dedup terms per doc in mapper
    term, _ = line.split('\t')

    # New term
    if current_term is not None and current_term != term:
        session.execute('INSERT INTO dfs (term, df) VALUES (%s, %s)', (current_term, df))
        df = 0

    current_term = term
    df += 1

if current_term is not None:
    session.execute('INSERT INTO dfs (term, df) VALUES (%s, %s)', (current_term, df))
