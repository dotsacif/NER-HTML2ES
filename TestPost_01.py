# -*- coding: utf-8 -*-
"""
Created on Sun Oct 31 20:45:28 2021

@author: Palmaria
"""

"""
import requests

url = 'https://www.w3schools.com/python/demopage.php'
myobj = {'somekey': 'somevalue'}

x = requests.post(url, data = myobj)

print(x.text)

"""

import requests
import json

r = requests.post(
    "http://localhost:4000/Ciclope.svc/Find",
    data=json.dumps({"usuario": "pepe","coleccion": "Decretos","anio": 2011,"passwd": "sesamo"}),
    headers={"Content-Type": "application/json"},
)

print(r.text)

"""
curl  -X POST http://localhost:4000/Ciclope.svc/Find -H 'Content-Type: application/json' -d'
{"usuario": "pepe","coleccion": "Decretos","anio": 2011,"passwd": "sesamo"}'

"""