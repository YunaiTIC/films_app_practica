import psycopg
from pelicula import Pelicula

class Persistencia_pelicula_pgSQL:
    def __init__(self, credentials):
        self.connection = psycopg.connect(**credentials)

    def llegeix(self, id_peli: int) -> Pelicula:
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM pelicula WHERE id=%s", (id_peli,))
        row = cursor.fetchone()
        cursor.close()
        if row:
            return Pelicula(row[0], row[1], row[2], row[3])
        return None

    def llegeix_de_disc(self, id_inici: int = None) -> list:
        cursor = self.connection.cursor()
        if id_inici:
            cursor.execute("SELECT * FROM pelicula WHERE id > %s ORDER BY id ASC LIMIT 10", (id_inici,))
        else:
            cursor.execute("SELECT * FROM pelicula ORDER BY id ASC LIMIT 10")
        rows = cursor.fetchall()
        cursor.close()
        return [Pelicula(row[0], row[1], row[2], row[3]) for row in rows]

    def grava(self, peli: Pelicula) -> bool:
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO pelicula (id, titulo, anyo, puntuacion) VALUES (%s, %s, %s, %s)",
                       (peli.id, peli.titulo, peli.anyo, peli.puntuacion))
        self.connection.commit()
        cursor.close()
        return True

    def modifica(self, peli: Pelicula) -> bool:
        cursor = self.connection.cursor()
        cursor.execute("UPDATE pelicula SET titulo=%s, anyo=%s, puntuacion=%s WHERE id=%s",
                       (peli.titulo, peli.anyo, peli.puntuacion, peli.id))
        self.connection.commit()
        cursor.close()
        return True

    def elimina(self, id_peli: int) -> bool:
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM pelicula WHERE id=%s", (id_peli,))
        self.connection.commit()
        cursor.close()
        return True

    def tanca(self):
        self.connection.close()
