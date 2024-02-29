#!/usr/bin/python3
# DEFINITIVO

import os
import yaml
import sys
import time
import json
import logging
import mysql.connector
from persistencia_pelicula_mysql import Persistencia_pelicula_mysql
from llistapelis import Llistapelis
from typing import List

# Connexió a la base de dades MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="pirineus",
    database="dam_m06"
)
cursor = conn.cursor()

THIS_PATH = os.path.dirname(os.path.abspath(__file__))
RUTA_FITXER_CONFIGURACIO = os.path.join(THIS_PATH, 'configuracio.yml')
print(RUTA_FITXER_CONFIGURACIO)

# Funció per obtenir la configuració des d'un fitxer YAML
def get_configuracio(ruta_fitxer_configuracio) -> dict:
    config = {}
    with open(ruta_fitxer_configuracio, 'r') as conf:
        config = yaml.safe_load(conf)
    return config

# Funció per obtenir les persistències de la base de dades segons la configuració
def get_persistencies(conf: dict) -> dict:
    credencials = {}
    if conf["base de dades"]["motor"].lower().strip() == "mysql":
        credencials['host'] = conf["base de dades"]["host"]
        credencials['user'] = conf["base de dades"]["user"]
        credencials['password'] = conf["base de dades"]["password"]
        credencials['database'] = conf["base de dades"]["database"]
        return {
            'pelicula': Persistencia_pelicula_mysql(credencials)
        }
    else:
        return {
            'pelicula': None
        }

# Funció per mostrar text lentament
def mostra_lent(missatge, v=0.05):
    for c in missatge:
        print(c, end='')
        sys.stdout.flush()
        time.sleep(v)
    print()

# Funció per mostrar el text de benvinguda
def landing_text():
    os.system('clear')
    print("Benvingut a la app de pel·lícules")
    time.sleep(1)
    msg = "Desitjo que et sigui d'utilitat!"
    mostra_lent(msg)
    input("Prem la tecla 'Enter' per a continuar")
    os.system('clear')

# Funció per mostrar una llista de pel·lícules
def mostra_llista(llistapelicula, start_index=0):
    os.system('clear')
    end_index = start_index + 10
    for movie in llistapelicula.pelicules[start_index:end_index]:
        print(json.dumps(json.loads(movie.toJSON()), indent=4))

# Funció per mostrar les següents pel·lícules
def mostra_seguents(llistapelicula, start_index):
    mostra_llista(llistapelicula, start_index + 10)

# Funció per mostrar el menú principal
def mostra_menu():
    print("0.- Surt de l'aplicació.")
    print("1.- Mostra les primeres 10 pel·lícules")
    print("3.- Inserir una nova pel·lícula.")
    print("4.- Modificar pel·lícula existent ")
    print("5.- Seleccionar pel·lícules per any.")

# Funció per mostrar el menú per a les següents 10 pel·lícules
def mostra_menu_next10():
    print("2.- Mostra les següents 10 pel·lícules")

# Funció per processar l'opció seleccionada
def procesa_opcio(context):
    return {
        "0": lambda ctx: mostra_lent("Fins la propera"),
        "1": lambda ctx: mostra_llista(ctx['llistapelis']),
        "2": lambda ctx: mostra_seguents(ctx['llistapelis'], ctx.get('start_index', 0)),
        "3": insereix_pelicula,
        "4": modifica_pelicula,
        "5": selecciona_perany
    }.get(context["opcio"], lambda ctx: mostra_lent("opció incorrecta!!!"))(context)

# Funció per inserir una nova pel·lícula
def insereix_pelicula(ctx):
    print("Has seleccionat inserir una nova pel·lícula. Insereix les dades:")
    ttl = input("Títol: ")
    anyo = int(input("Any: "))
    puntuacio = float(input("Puntuació: "))
    votos = int(input("Vots: "))

    # Comprovem que el títol no existeixi
    query_select = "SELECT * FROM PELICULA WHERE TITULO = %s"
    cursor.execute(query_select, (ttl,))
    existeix = cursor.fetchone()

    if existeix:
        print("Aquesta pel·lícula ja existeix")
    else:
        query_insert = "INSERT INTO PELICULA (TITULO, ANYO, PUNTUACION, VOTOS) VALUES (%s, %s, %s, %s)"
        cursor.execute(query_insert, (ttl, anyo, puntuacio, votos))
        conn.commit()
        print("Pel·lícula inserida")

# Funció per modificar una pel·lícula existent
def modifica_pelicula(ctx):
    print("Has seleccionat modificar els detalls d'una pel·lícula existent.")
    ttl = input("Títol exacte: ")
    np = float(input("Nova puntuació: "))
    nv = int(input("Nous vots: "))

    query = "UPDATE PELICULA SET PUNTUACION = %s, VOTOS = %s WHERE TITULO = %s"
    cursor.execute(query, (np, nv, ttl))
    conn.commit()

# Funció per seleccionar pel·lícules per any, puntuació o actors
def selecciona_perany(ctx):
    print("Has seleccionat consultar la base de dades de pel·lícules.")
    opc = int(input("Vols consultar mitjançant un rang d'anys (1), per puntuació(2), o mitjançant els actors que hi participen (3)?  "))

    if opc == 1:
        any1 = int(input("Any mínim: "))
        any2 = int(input("Any màxim: "))

        query = "SELECT * FROM PELICULA WHERE ANYO > %s AND ANYO < %s"
        cursor.execute(query, (any1, any2))
        rows = cursor.fetchall()

        for row in rows:
            print(row)

    elif opc == 2:
        punt = float(input("Indica el valor: "))
        z = input("Vols veure les pel·lícules amb més (a) o menys (b) valor que el que has introduït?   ")
        if z == "a":
            query = "SELECT * FROM PELICULA WHERE PUNTUACION > %s"
            cursor.execute(query, (punt,))
        elif z == "b":
            query = "SELECT * FROM PELICULA WHERE PUNTUACION < %s"
            cursor.execute(query, (punt,))

        rows = cursor.fetchall()

        for row in rows:
            print(row)

    elif opc == 3:
        ttl = input("Títol: ")

        query = """
            SELECT A.NOMBRE AS NombreActor
            FROM ACTOR A
            JOIN REPARTO R ON A.ID = R.ACTOR_ID
            JOIN PELICULA P ON R.PELICULA_ID = P.ID
            WHERE P.TITULO = %s
        """
        cursor.execute(query, (ttl,))
        rows = cursor.fetchall()

        for row in rows:
            print(row)

# Funció per llegir les dades de la base de dades
def database_read(id_inicio: int = None):
    logging.basicConfig(filename='pelicules.log', encoding='utf-8', level=logging.DEBUG)
    config = get_configuracio(RUTA_FITXER_CONFIGURACIO)
    persistencies = get_persistencies(config)

    films = Llistapelis(
        persistencia_pelicula=persistencies['pelicula']
    )
    films.llegeix_de_disc(id_inicio)
    return films

# Funció del bucle principal de l'aplicació
def bucle_principal(context):
    opcio = None

    while opcio != '0':
        mostra_menu()
        opcio = input("Selecciona una opció: ")
        context["opcio"] = opcio

        if context["opcio"] == '1':
            id_inicio = None
            films = database_read(id_inicio)
            context["llistapelis"] = films
            mostra_llista(films)
            mostra_menu_next10()
            context['start_index'] = 0

        elif context["opcio"] == '2':
            if 'llistapelis' in context:
                mostra_seguents(context['llistapelis'], context.get('start_index', 0))
                context['start_index'] += 10
                continue  

        elif context["opcio"] == '3':
            insereix_pelicula(context)

        elif context["opcio"] == '4':
            modifica_pelicula(context)

        elif context["opcio"] == '5':
            selecciona_perany(context)

# Funció principal de l'aplicació
def main():
    context = {
        "llistapelis": None
    }
    landing_text()
    bucle_principal(context)

if __name__ == "__main__":
    main()
