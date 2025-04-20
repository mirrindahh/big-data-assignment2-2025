from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect('bm25_index')

# We do not use index name, but we need a primary key
session.execute('''\
CREATE TABLE IF NOT EXISTS dl_avg (
    index_name TEXT PRIMARY KEY,
    dl_avg DOUBLE
)''')

count = 0
dl_avg = 0
for row in session.execute('SELECT dl FROM docs'):
    count += 1
    dl_avg += row.dl

dl_avg /= count
print(f'Average dl: {dl_avg}')

session.execute('''INSERT INTO dl_avg (index_name, dl_avg) VALUES (%s, %s)''', ("index", dl_avg))