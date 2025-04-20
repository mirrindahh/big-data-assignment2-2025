import math
import string
import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

cluster = Cluster(['cassandra-server'])
session = cluster.connect('bm25_index')

spark = SparkSession.builder \
    .appName('query') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

# Save as mapper1.py
alphabet = set(string.ascii_lowercase)
query_terms = []
for term in sys.argv[1].split():
    term = ''.join(c for c in term.lower() if c in alphabet)
    if len(term) != 0:
        query_terms.append(term)

print("sys.argv: ",sys.argv[1])
print("query_terms: ",query_terms)
dl_avg = session.execute('SELECT dl_avg FROM dl_avg').one().dl_avg
doc_ids = [row.doc_id for row in session.execute("SELECT doc_id FROM docs")]
doc_ids_rdd = spark.sparkContext.parallelize(doc_ids)

k1 = 1
b = 0.75
N = doc_ids_rdd.count()

def bm25_score(doc_id):
    # Each worker should have its own
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('bm25_index')

    dl = session.execute('SELECT dl FROM docs WHERE doc_id=%s', (doc_id,)).one().dl

    score = 0
    for term in query_terms:
        df = session.execute('SELECT df FROM dfs WHERE term=%s', (term,)).one()

        # No such term in entire index
        if df is None:
            continue

        df = df.df
        tf = session.execute('SELECT tf FROM tfs WHERE doc_id=%s AND term=%s', (doc_id, term)).one()

        # No such term in the given doc
        if tf is None:
            continue

        tf = tf.tf

        idf = math.log10(N / df)
        numerator = (k1 + 1) * tf
        denominator = k1 * ((1 - b) + b * (dl / dl_avg)) + tf
        score += idf * (numerator / denominator)

    return (doc_id, score)

bm25_rdd = doc_ids_rdd.map(bm25_score)
bm25_top10_rdd = bm25_rdd.takeOrdered(10, key=lambda x: -x[1])

for doc_id, score in bm25_top10_rdd:
    print(doc_id, score, session.execute('SELECT doc_title FROM docs WHERE doc_id=%s', (doc_id,)).one().doc_title)
