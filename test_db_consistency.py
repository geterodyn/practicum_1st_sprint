import sqlite3
import psycopg2
from os import environ
from dotenv import load_dotenv
from contextlib import closing
from dateutil.parser import parse
from load_data import Filmwork, Person, Genre, GenreFilmwork, PersonFilmwork, TABLES

load_dotenv()

SQLITE_DB = environ.get('SQLITE_DB')
DB_NAME = environ.get('DB_NAME')
DB_USER = environ.get('DB_USER')
DB_PASSWORD = environ.get('DB_PASSWORD')
DB_HOST = environ.get('DB_HOST', '127.0.0.1')
DB_PORT = environ.get('DB_PORT', 5432)
POSTGRES_DSN = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD, 'host': DB_HOST, 'port': DB_PORT}


class DBLoader:
    def __init__(self, connection):
        self.connection = connection

    def data_class(self, table):
        data_class = None
        if table == 'film_work':
            data_class = Filmwork
        elif table == 'person':
            data_class = Person
        elif table == 'genre':
            data_class = Genre
        elif table == 'person_film_work':
            data_class = PersonFilmwork
        elif table == 'genre_film_work':
            data_class = GenreFilmwork
        return data_class

    def get_all_data(self, table, batch_size=10, is_pg = False):
        all_data = []
        query = f"SELECT * FROM {table};"
        if not is_pg:
            cur = self.connection.cursor()
            cur.row_factory = sqlite3.Row
        else:
            cur = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query)
        data_class = self.data_class(table)
        while True:
            data = cur.fetchmany(batch_size)
            if not data:
                break
            for entry in data:
                dc_object = data_class(**entry)
                if not is_pg:
                    if 'created_at' in dict(entry):
                        dc_object.created_at = parse(dc_object.created_at)
                    if 'updated_at' in dict(entry):
                        dc_object.updated_at = parse(dc_object.updated_at)
                all_data.append(dc_object)
        return all_data


def check_get_all_data(sqlite_conn, pg_conn):
    pg_loader = DBLoader(pg_conn)
    sqlite_loader = DBLoader(sqlite_conn)
    print(sqlite_loader.get_all_data(TABLES[0])[0].created_at == pg_loader.get_all_data(TABLES[0], is_pg=True)[0].created_at)



def entry_number(conn, table):
    cur = conn.cursor()
    query = f"SELECT COUNT(*) FROM {table};"
    cur.execute(query)
    entry_number = cur.fetchone()[0]
    return entry_number


def test_db_consistency():
    with closing(sqlite3.connect(SQLITE_DB)) as sqlite_conn, closing(psycopg2.connect(**POSTGRES_DSN)) as pg_conn:
        sqlite_loader = DBLoader(sqlite_conn)
        pg_loader = DBLoader(pg_conn)
        for table in TABLES:
            sqlite_data = sqlite_loader.get_all_data(table)
            pg_data = pg_loader.get_all_data(table, is_pg=True)
            assert sqlite_data == pg_data


def test_entry_number():
    with closing(sqlite3.connect(SQLITE_DB)) as sqlite_conn, closing(psycopg2.connect(**POSTGRES_DSN)) as pg_conn:
        for table in TABLES:
            assert entry_number(sqlite_conn, table) == entry_number(pg_conn, table)

if __name__ == "__main__":
    with closing(sqlite3.connect(SQLITE_DB)) as sqlite_conn, closing(psycopg2.connect(**POSTGRES_DSN)) as pg_conn:
        check_get_all_data(sqlite_conn, pg_conn)

