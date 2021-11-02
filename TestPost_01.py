
import requests
import json

r = requests.post(
    "http://localhost:4000/Ciclope.svc/Find",
    data=json.dumps({"usuario": "pepe","coleccion": "Decretos","anio": 2011,"passwd": "sesamo","fields": "title_orig,numero"}),
    headers={"Content-Type": "application/json"},
)

print(r.text)

