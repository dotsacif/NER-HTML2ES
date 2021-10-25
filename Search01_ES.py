# -*- coding: utf-8 -*-
"""
curl -XPOST localhost:9200/index_local/my_doc_type/_bulk --data-binary  @/home/data1.json
https://stackoverflow.com/questions/33340153/elasticsearch-bulk-index-json-data

https://kb.objectrocket.com/elasticsearch/how-to-use-python-helpers-to-bulk-load-data-into-an-elasticsearch-index

http://suin-juriscol.gov.co/index.html

curl -X GET "localhost:9200/minjusticia/_mapping?pretty"

https://elasticsearch-py.readthedocs.io/en/master/api.html
https://medium.com/naukri-engineering/elasticsearch-tutorial-for-beginners-using-python-b9cb48edcedc


ATENCION+   Ver...
"ignore_above": 256

PARA INSTALAR pip 
file:///C:/Windows/System32/get-pip.py

> # "...what I have to do on every system restart or docker desktop update:
# open powershell
# wsl -d docker-desktop
# sysctl -w vm.max_map_count=262144
>docker-compose -f docker-compose-data.yml up

EN PREPRODUCCION 192.168.1.47 es DISCO "C" donde estan las exportaciones
"""
import elasticsearch
import os
import codecs
import sys
import time

"""
Modulo de definiciones de ElasticSearch
"""

INDEX_NAME = 'minjusticia'
TYPE_NAME = 'ciclope'
ID_FIELD = 'cms_id'

try:
        """
        Cambiar aca Desarrollo x Produccion
        """
        # Desarrollo 
        es = elasticsearch.Elasticsearch([{'host': '192.168.8.73', 'port': 9200}])
        # Produccion
        #es = elasticsearch.Elasticsearch([{'host': '192.168.8.73', 'port': 9200}])

except:
        print("Error inicio ElasticSearch ")   
        sys.exit()

"""
Modulo sanity check
"""
print("=====================================================================")
body01 = '{"query": { "match": { "anio": "2011"  }  } }'
res = es.search(index='minjusticia',body=body01)
print(body01)

print("%d documents found" % res['hits']['total']['value'])
#for doc in res['hits']['hits']:
#    trozo = doc['_source']['anio']
#    print("%s--->%s " % (doc['_id'],trozo[:300]))
#    #print("%s---> %s %s" % (doc['_id'], doc['_source']['numero'],doc['_source']['id_orig']))
#    continue
    
print("=====================================================================")
body01 = '{"query": { "match": { "numero_proviencia": "1149"  }  } }'
res = es.search(index='minjusticia',body=body01)
print(body01)
print("%d documents found" % res['hits']['total']['value'])
#for doc in res['hits']['hits']:
#    trozo = doc['_source']['numero_proviencia']
#    print("%s--->%s " % (doc['_id'],trozo[:300]))
#    #print("%s---> %s %s" % (doc['_id'], doc['_source']['numero'],doc['_source']['id_orig']))
#    continue
print("=====================================================================")
body01 = '{"query": { "match": { "coleccion": "CorteConstitucional"  }  } }'
res = es.search(index='minjusticia',body=body01)
print(body01)
print("%d documents found" % res['hits']['total']['value'])
print("=====================================================================")


