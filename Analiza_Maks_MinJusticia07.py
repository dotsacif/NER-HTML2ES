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
from datetime import datetime
from bs4 import BeautifulSoup
from distutils.dir_util import copy_tree
import shutil
import time
from shutil import rmtree


plataforma = sys.platform
if plataforma == 'win32':
    cadena_windows = "chcp 65001"
    os.system(cadena_windows)
    #sys.exit()

now = datetime.now()
el_epoch = str(int(time.mktime(now.timetuple())))

print("Empieza :"+time.strftime("%H:%M:%S")) #Formato de 24 horas
inicial = time.strftime("%H:%M:%S")
horario = str(time.strftime("%H:%M:%S"))

archiBulk = codecs.open(".."+os.sep+"Bulk"+el_epoch+".json", "w","utf-8") # Creo archivo Json en un nivel mas arriba
archiErr = codecs.open(".."+os.sep+"ERR_"+el_epoch+".txt", "w+","utf-8") # Creo archivo de erroresen un nivel mas arriba

#os.chdir("M:\\exportNew")
os.chdir("D:\\exportNew")

osCurrent = os.getcwd()

#DIRECTORIO_ORIGEN = "M:/exportNew"
#DIRECTORIO_DESTINO = "M:\\exportados\\htmls"


DIRECTORIO_ORIGEN = "D:/exportNew"
DIRECTORIO_DESTINO = "D:\\exportados\\htmls"



try:
    rmtree(DIRECTORIO_DESTINO)
except:
    print("No Borraaa")
    pass

os.makedirs(DIRECTORIO_DESTINO, exist_ok=True)

#time.sleep(20)

print("Copiando...")

#copy_tree(DIRECTORIO_ORIGEN, DIRECTORIO_DESTINO)
print("Copiado")

print("Termino el Copiado :"+time.strftime("%H:%M:%S")) #Formato de 24 horas
inicial = time.strftime("%H:%M:%S")
horario = str(time.strftime("%H:%M:%S"))



"""
Modulo de definiciones de ElasticSearch
"""
ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'minjusticia'
TYPE_NAME = 'ciclope'
ID_FIELD = 'cms_id'

try:
        es = elasticsearch.Elasticsearch([{'host': 'localhost', 'port': 9200}])
        #es.indices.delete(index='minjusticia', ignore=[400, 404])
        
        # delete index if exists
        if es.indices.exists(INDEX_NAME):
            es.indices.delete(index=INDEX_NAME,ignore=[400, 404])
        
            
        # index settings
        request_body = {
                "settings" : {
                    "number_of_shards": 5,
                    "number_of_replicas": 1
                },
        
                'mappings': {
                        'properties': {
                            'subtipo': {'type': 'text'},
        	                'es_codigo':  {'type': 'text'},
        	                'estado_documento':  {'type': 'text'},
        	                'asunto':  {'type': 'text'},
        	                'cms_id': {'type': 'long'},
                            'materia' :  {'type': 'text'},
                            'sector' : {'type': 'text'},
                            'entidad_emisora' : {'type': 'text'},
                            'tipo' : {'type': 'text'},
                            'estado' : {'type': 'text'},
                            'epigrafe' : {'type': 'text'},
                            'numero' : {'type': 'text'},
                            'name' : {'type': 'text'},
                            'anio' : {'type': 'text'},
                            'es_estatuto' : {'type': 'text'},
                            'titulo_uniforme' : {'type': 'text'},
                            'Estatutos' : {'type': 'text'},
                            'id_orig' : {'type': 'text'},
                            'title_orig' : {'type': 'text'},
                            'html_orig' : {'type': 'text'},
                            'texto_plano' : {'type': 'text'}, 
                            'tipo_providencia_consejo_est' : {'type': 'text'},
                            'accionado': {'type': 'text'},
                            'accionante': {'type': 'text'},
                            'anio_providencia': {'type': 'text'},
                            'consejero_ponente': {'type': 'text'},
                            'demandado': {'type': 'text'},
                            'demandante': {'type': 'text'},
                            'fecha': {'type': 'text'},
                            'fecha_notificacion': {'type': 'text'},
                            'fecha_providencia': {'type': 'text'},
                            'fecha_sentencia': {'type': 'text'},
                            'gaceta': {'type': 'text'},
                            'magistrado_ponente': {'type': 'text'},
                            'norma_demandada': {'type': 'text'},
                            'numero_ext_radicacion': {'type': 'text'},
                            'numero_int_radicacion': {'type': 'text'},
                            'numero_proviencia': {'type': 'text'},
                            'numero_radicacion_proceso': {'type': 'text'},
                            'tipo_providencia_corte': {'type': 'text'},
                            'coleccion': {'type': 'text'},
                            
        
                        }}
            }
        print("creando 'Ciclope' index...")
        
        # create an index, ignore if it exists already
        res1 = es.indices.create(index = INDEX_NAME,  body = request_body, ignore=400)

except:
        print("Error inicio ElasticSearch ")   
        archiErr.write("Error inicio ElasticSearch "+"\n")     
        archiErr.close()
        archiBulk.close()
        sys.exit()

bulk_data = [] 
contaLinea = 1

"""
Fin modulo ES
"""

#print(" Arranco ")
apareos = dict()

maks =[]
contenido = os.listdir(osCurrent)
for fichero in contenido:
    #print(fichero)
    es_xml = fichero.find('.xml')
    if es_xml > 0:
        maks.append(fichero)

for Arkivo in maks:
   print("Trabajo la Mak "+Arkivo)    
   laMak = Arkivo

   try:
        archivo  = codecs.open(Arkivo, "r", "utf-8")
   except:
        print("Error inesperado "+">>>>>>"+laMak)   
        archiErr.write("Error inesperado "+">>>>>>"+laMak+"\n") 
        #sys.exit()
        continue
        
   linea0="..."
   
   anio   = " "
   numero = " "
   tipo   = " "
   estado = " "
   estado_documento = " "
   epigrafe = " "
   name = " "
   coleccion = " "
   id_orig  = " "
   title_orig = " "
   html_orig = " "    
   subtipo = " " 
   es_codigo = " " 
   nombre_codigo = " " 
   asunto = " " 
   materia = " " 
   sector = " " 
   entidad_emisora = " "
   tipo_providencia_consejo_est  =" "  
   cms_id = " "
   texto_plano = " "
   accionado= " "
   accionante= " "
   anio_providencia= " "
   consejero_ponente= " "
   demandado= " "
   demandante= " "
   fecha= " "
   fecha_notificacion= " "
   fecha_providencia= " "
   fecha_sentencia= " "
   gaceta= " "
   magistrado_ponente= " "
   norma_demandada= " "
   numero_ext_radicacion= " "
   numero_int_radicacion= " "
   numero_proviencia= " "
   numero_radicacion_proceso= " "
   tipo_providencia_corte= " "
   koleccion = " "


   while linea0 != '':
    # procesar línea
    linea0=archivo.readline()
    #print("proceso linea de la MAK ",linea0)
    #print(linea0)
    #name = input("---------------------- ")
    
    inicio2 = linea0.find('content-collection id=')
    if inicio2 >0:
       final2  = linea0.find('title=',inicio2)
       koleccion  = linea0[inicio2+23:final2-2]
    
    inicio = linea0.find('location=')
    if inicio >0:
        final  = linea0.find('.html',inicio)
        NomeFile = linea0[inicio+10:final+5]

        inicio = NomeFile.rfind('\\')
        NomeFile2 = NomeFile[inicio+1:]
         
        """
        BUSCO ID del documento EN LA MAK
        """
        inicio = linea0.find('id=')
        NomeFile = linea0[inicio+4:]
        final = NomeFile.find('"/>')
        id_orig = NomeFile[:final]
  
        
        """
        BUSCO TITLE del documento EN LA MAK
        """
        inicio = linea0.find('title=')
        NomeFile = linea0[inicio+7:]
        final = NomeFile.find('"')
        title_orig = NomeFile[:final]

        
        """
        BUSCO LOCATION ORIGINAL
        """
        inicio = linea0.find('location=')
        NomeFile = linea0[inicio+10:]
        final = NomeFile.find(".html")
        html_orig = NomeFile[:final+5]

      
        Arkivo2 = "D:/exportNew/"+coleccion+"/"+NomeFile2
        
        Arkivo2 = html_orig
        #destino2 = Arkivo2.replace("exportNew","Nuevos")
        #shutil.copytree(Arkivo2, destino2) 
        #sys.exit()  
        
        try:
            archiIn  = codecs.open(Arkivo2, "r", "utf-8")
            #destino = "D:\\htmls\\"+NomeFile2
            # try:
            #     #shutil.copy(Arkivo2, destino)
            #     destino2 = Arkivo2.replace("exportNew","Nuevos")
            #     shutil.copytree(Arkivo2, destino2) 
            #     sys.exit()
            #     #https://www.geeksforgeeks.org/python-shutil-copytree-method/
            # except:
            #     print("error al grabar ccccccccccccccccc",destino)
            #     pass   
                
        except:
            #print("Error al open de  "+">>>>>>"+Arkivo2)   
            archiErr.write("Error al open de  "+">>>>>>"+Arkivo2+" en la mak "+laMak+"\n")  
            #sys.exit()  
            continue
        
        # print("nnn")
        # sys.exit()    
            
        with open(Arkivo2, 'r', encoding='utf-8') as f:  ## levanto el HTML para indexar por palabra libre
             contenido_html = f.read()    
             
        linea=".."
        # Empiezo a procesar los HTMLS.
        apareos.clear()
        while linea != '':
         try:
            linea=archiIn.readline()
         except (IOError, ValueError):
            #print("Un I/O error o ValueError sucedio >>> "+">>>>>>"+Arkivo)
            archiErr.write("Un I/O error o ValueError sucedio >>> "+">>>>>>"+Arkivo+"\n") 
            continue
         except:
            #print("Error inesperado "+">>>>>>"+Arkivo)   
            archiErr.write("Error inesperado "+">>>>>>"+Arkivo+"\n") 
            continue
    
         campos = ['<span field="subtipo">','<span field="es_codigo">','<span field="nombre_codigo">','<span field="estado_documento">','<span field="asunto">','<span field="materia">','<span field="sector">','<span field="entidad_emisora">','<span field="tipo">','<span field="estado">','<span field="epigrafe">','<span field="numero">','<span field="name">','<span field="anio">','<span field="es_estatuto">','<span field="titulo_uniforme">','<span field="Estatutos">','<span field="tipo_providencia_consejo_est">','<span field="cms_id">','<span field="accionado">','<span field="accionante">','<span field="anio_providencia">','<span field="consejero_ponente">','<span field="demandado">','<span field="demandante">','<span field="fecha">','<span field="fecha_notificacion">','<span field="fecha_providencia">','<span field="fecha_sentencia">','<span field="gaceta">','<span field="magistrado_ponente">','<span field="norma_demandada">','<span field="numero_ext_radicacion">','<span field="numero_int_radicacion">','<span field="numero_proviencia">','<span field="numero_radicacion_proceso">','<span field="tipo_providencia_corte">']
         for campo in campos:

             campito = campo.replace('<span field="','')
             campito = campito.replace('">','')
             negativo = campo+"</span>"
            
             if linea.find(campo) > 0 and linea.find(negativo) < 0:
                 inicio = linea.find(campo)
                 largo = len(campo)
                 resto = linea[inicio:]
                 fin = resto.find('</span>')
                 payload = resto[largo:fin]
                 payload = payload.replace("'","")
                 # convertir una cadena en variable
                 cadena = campito
                 try:
                     exec(cadena+"='"+payload+"'")
                     apareos[campito] = payload
                 except:
                     pass
                     
 
        cadena = "{"
        for k,v in apareos.items():
            cadena = cadena +'"' +k + '"' + ':' +'"' + v + '",' 
        
        #cadena = cadena[:-1] +"}"
        #html_orig = html_orig.replace('\\','\\') 
        #print("-==================0------------------------------")
        #print(html_orig)
        #print("-==================0------------------------------")

        cadena = cadena +'"id_orig":'+ '"' +id_orig +'"' +', "title_orig": '+'"' +title_orig +'","html_orig":'+'"' +html_orig +'"' # +"}"
        apareos["id_orig"] = id_orig
        apareos["title_orig"] = title_orig
        apareos["html_orig"] = html_orig
        apareos["coleccion"] = koleccion
        
        
        soup = BeautifulSoup(contenido_html,'html.parser')
        for data in soup(['style', 'script']):
            # Remove tags
            data.decompose()
            
        texto_plano =  ' '.join(soup.stripped_strings)
        
        #https://www.geeksforgeeks.org/remove-all-style-scripts-and-html-tags-using-beautifulsoup/
        #texto_plano = str(soup.get_text())
 
        cadena = cadena +","+'"texto_plano":'+'"'+texto_plano+'"'+"}"
        apareos["texto_plano"] = texto_plano  
        #sys.exit()         
        #sys.exit()
        el_id = str(contaLinea)

        op_dict = {"index":{"_index": INDEX_NAME, "_id":el_id }  }
        
        bulk_data.append(op_dict)
        bulk_data.append(apareos)

        #res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
        #bulk_data = []
        #print(contaLinea," ",Arkivo2)
        
        #if len(bulk_data) > 2000: #	 attenti que van de a pares...
        if len(bulk_data) > 200: #	 attenti que van de a pares...
            #print("Grabando el BULK....")
            try:
                res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
                bulk_data = []
                print("Grabo el BULK....")
            except:
                print("Error ---   No funciono EL BULK 200 ElasticSearch ")   
                archiErr.write("Error BULK 200 ElasticSearch  "+Arkivo2+"\n") 
                bulk_data = []
                        
        contaLinea = contaLinea + 1
       
        #archiBulk.write('{"index":{}}'+"\n")
        #archiBulk.write(cadena+"\n")
        
if len(bulk_data) > 0:
    print("bulk ULTIMO indexing...")
    try:
        res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
    except:
        print("Error No funciono BULK FinalElasticSearch ")   
        archiErr.write("Error No funciono BULK Final ElasticSearch  "+"\n") 
        bulk_data = []


print("Finalizo la Carga -------------------")  
print("\n\n\nCantidad archivos HTML procesados",contaLinea)  


"""
Modulo sanity check
"""
try:
    res = es.search(index = INDEX_NAME, size=2000, body={"query": {"match_all": {}}})
    print("%d SANITY CHECK documents found" % res['hits']['total']['value'])
    for doc in res['hits']['hits']:
        trozo = doc['_source']['html_orig']
        print("%s--->%s " % (doc['_id'],trozo[:300]))
        #print("%s---> %s %s" % (doc['_id'], doc['_source']['numero'],doc['_source']['id_orig']))
        continue
except:
        print("Error No funciono Sanity Check ElasticSearch ")   
        archiErr.write("Error No funciono Check ElasticSearch  "+"\n")     

"""
FINAL
Modulo sanity check
"""

print("Inicio:"+str(inicial))    
print("Termina:"+time.strftime("%H:%M:%S")) 

archiErr.close()
archiBulk.close()

"""
curl -XGET localhost:9200/_cat/indices?v
"""
  