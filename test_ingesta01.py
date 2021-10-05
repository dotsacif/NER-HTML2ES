import elasticsearch
import os
import sys

os.chdir("C:\\exportNew")

osCurrent = os.getcwd()

"""
Modulo de definiciones de ElasticSearch
"""
ES_HOST = {"host" : "192.168.8.73", "port" : 9200}
INDEX_NAME = 'minjusticia'
TYPE_NAME = 'ciclope'
ID_FIELD = 'cms_id'

try:
        es = elasticsearch.Elasticsearch([{'host': '192.168.8.73', 'port': 9200}])
        #es.indices.delete(index='minjusticia', ignore=[400, 404])
except:
        print("Error inicio ElasticSearch ")   
        sys.exit()

"""
Modulo sanity check
"""
try:
    res = es.search(index = INDEX_NAME, size=2000, body={"query": { "match": { "magistrado_ponente": "MAURICIO GONZÁLEZ CUERVO" }}})
    
    ##'{  "query": { "match": { "magistrado_ponente": "MAURICIO GONZÁLEZ CUERVO" } 
    print("%d SANITY CHECK documents found" % res['hits']['total']['value'])
    for doc in res['hits']['hits']:
        trozo = doc['_source']['html_orig']
        print("%s--->%s " % (doc['_id'],trozo[:300]))
        #print("%s---> %s %s" % (doc['_id'], doc['_source']['numero'],doc['_source']['id_orig']))
        continue
except:
        print("Error No funciono Sanity Check ElasticSearch ")   
 