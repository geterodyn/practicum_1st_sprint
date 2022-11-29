import os
import uuid
import sqlite3
from datetime import datetime
from dataclasses import dataclass, asdict
from contextlib import closing

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', 5432)
SQLITE_DB = os.environ.get('SQLITE_DB', 'db.sqlite')

TABLES = ['genre', 'person', 'film_work', 'genre_film_work', 'person_film_work']

@dataclass
class Filmwork:
    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime
    file_path: str
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime


@dataclass
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created_at: datetime
    updated_at: datetime


@dataclass
class Person:
    id: uuid.UUID
    full_name: str
    created_at: datetime
    updated_at: datetime


@dataclass
class GenreFilmwork:
    id: uuid.UUID
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    created_at: datetime


@dataclass
class PersonFilmwork:
    id: uuid.UUID
    person_id: uuid.UUID
    film_work_id: uuid.UUID
    role: str
    created_at: datetime


class SQLiteLoader:

    def __init__(self, connection):
        self.cursor = connection.cursor()

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

    def load_movies(self, table, batch_size:int = 10):
        query = f"SELECT * FROM {table}"
        self.cursor.row_factory = sqlite3.Row
        self.cursor.execute(query)
        data_class = self.data_class(table)
        while True:
            batch = self.cursor.fetchmany(size=batch_size)
            if not batch:
                break
            else:
                dc_objects = [data_class(**entry) for entry in batch]
                yield dc_objects     


class PostgresSaver:

    def __init__(self, connection):
        self.cursor = connection.cursor()


    def save_data(self, table, data_batch) -> None:
        for obj_list in data_batch:
            for obj in obj_list:
                entry = asdict(obj)
                headers = ', '.join(entry.keys())
                values = list(entry.values())
                values_string = ("%s, " * len(entry)).rstrip(', ')
                query = f"INSERT INTO {table} ({headers}) VALUES ({values_string}) ON CONFLICT (id) DO NOTHING;"
                self.cursor.execute(query, values)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)
    for table in TABLES:
        data_batch = sqlite_loader.load_movies(table)
        postgres_saver.save_data(table, data_batch)
        pg_conn.commit()
    

if __name__ == "__main__":
    dsn = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD, 'host': DB_HOST, 'port': DB_PORT}
    with closing(sqlite3.connect(SQLITE_DB)) as sqlite_conn, closing(psycopg2.connect(**dsn, cursor_factory=DictCursor)) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
        



