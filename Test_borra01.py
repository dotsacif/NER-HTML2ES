# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 13:54:06 2021

@author: GOCSyS
"""
import sys
from distutils.dir_util import copy_tree
from datetime import datetime
from shutil import rmtree
import time
import os

DIRECTORIO_ORIGEN = "C:/aviones"
DIRECTORIO_DESTINO = "C:/Cerdos/Aviones"

try:
    rmtree(DIRECTORIO_DESTINO)
except:
    print("No Borraaa")
    pass

os.makedirs(DIRECTORIO_DESTINO, exist_ok=True)

#time.sleep(20)

print("Copiando...")

copy_tree(DIRECTORIO_ORIGEN, DIRECTORIO_DESTINO)
print("Copiado")



