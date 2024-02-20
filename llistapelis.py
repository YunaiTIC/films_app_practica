#!/bin/usr/python3

import json
from typing import List
from ipersistencia_pelicula import IPersistencia_pelicula
from pelicula import Pelicula

class Llistapelis():
    def __init__ (self, persistencia_pelicula: IPersistencia_pelicula) -> None:
        self._pelicules = []
        self._ult_id = 0
        self._persistencia_pelicula = persistencia_pelicula
        
    @property
    def pelicules(self) -> List[Pelicula]:
        return self._pelicules
    
    @property
    def ult_id(self) -> int:
        return self._ult_id

    @property
    def persistencia_pelicula(self) -> IPersistencia_pelicula:
        return self._persistencia_pelicula
    
    def __repr__(self):
        return self.toJSON()
    
    def toJSON(self):
        pelicules_dict = []
        for pelicula in self._pelicules:
            pelicules_dict.append(json.loads(pelicula.toJSON()))
        self_dict = {
            "pelicules": pelicules_dict
            }   
        return json.dumps(self_dict)

    
    def llegeix_de_disc(self, id:int):
        self._pelicules = self.persistencia_pelicula.totes_pag(id)
        self._ult_id = id

        

