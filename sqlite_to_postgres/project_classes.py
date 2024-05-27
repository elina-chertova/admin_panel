import csv
import datetime
import logging
import os
import shutil
import sqlite3
from contextlib import contextmanager
from dataclasses import astuple

import psycopg2
import psycopg2.extras


class SQLiteLoader:
    def __init__(self, table_map: dict, db_path: str, data_folder: str, batch_size: int):
        self.table_map = table_map
        self.db_path = db_path
        self.batch_size = batch_size
        self.data_folder = data_folder

    def iter_row(self, cursor):
        while True:
            rows = cursor.fetchmany(self.batch_size)
            if not rows:
                break
            for row in rows:
                yield row

    @contextmanager
    def conn_context(self, table: str):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = self.table_map[table].row_factory
        yield conn
        conn.close()

    def extract_data_from(self, table: str):
        logging.basicConfig(level=logging.DEBUG, filename='project.log', format='%(asctime)s %(levelname)s:%(message)s')
        try:
            with self.conn_context(table) as conn:

                curs = conn.cursor()
                curs.execute("SELECT * FROM {0};".format(table))
                for row in self.iter_row(curs):
                    with open(self.data_folder + table + '.csv', mode="a", encoding='utf-8') as file:
                        file_writer = csv.writer(file, delimiter=";", lineterminator="\r")
                        file_writer.writerow(astuple(row))

        except sqlite3.OperationalError as e:
            logging.error("Didn't find the table '{0}'".format(table))
            raise


class PostgresSaver:
    def __init__(self, db_settings: dict, table: str, table_map: dict):
        self.db_settings = db_settings
        self.table = table
        self.table_map = table_map

    def query_settings(self):
        fields = ','.join(attr for attr in self.table_map[self.table].__annotations__.keys()).replace('file_path,', '')
        ids = 'film_work_id, genre_id' if self.table == 'genre_film_work' else 'id'
        query = 'INSERT INTO content.{0} ({1}) VALUES %s ON CONFLICT ({2}) DO NOTHING;'.format(self.table, fields, ids)
        return query

    def fields_settings(self, row: tuple):
        fields = ()
        if self.table == 'film_work':
            fields = (row[0], row[1], row[2], row[3] or None, row[5] or None, row[6],
                      datetime.datetime.now(), datetime.datetime.now())
        if self.table == 'genre':
            fields = (row[0], row[1], row[2] or None, datetime.datetime.now(), datetime.datetime.now())
        if self.table == 'person':
            fields = (row[0], row[1], datetime.datetime.now(), datetime.datetime.now())
        if self.table == 'person_film_work':
            fields = (row[0], row[1], row[2], row[3], datetime.datetime.now())
        if self.table == 'genre_film_work':
            fields = (row[0], row[1], row[2], datetime.datetime.now())
        return fields

    def insert_n_data_to_db(self, table_files):
        logging.basicConfig(level=logging.DEBUG, filename='project.log', format='%(asctime)s %(levelname)s:%(message)s')
        try:
            with psycopg2.connect(**self.db_settings) as conn:
                with conn.cursor() as curs:
                    query = self.query_settings()
                    for item in table_files:
                        with open(item, 'r') as f:
                            read_csv = csv.reader(f, delimiter=";")
                            tuples = [self.fields_settings(tuple(row)) for row in read_csv]

                        psycopg2.extras.execute_values(curs, query, tuple(tuples))

            conn.close()

        except psycopg2.OperationalError:
            logging.error("Incorrect credentials for database")
            raise


def write_chunk(part, lines, path):
    with open(path + 'out' + str(part) + '.csv', 'w') as f_out:
        f_out.writelines(lines)


class PrepareDataset:
    def __init__(self, path_to_tmp_csv: str, path_to_main_csv: str, chunk_size: int = 90):
        self.path_to_tmp_csv = path_to_tmp_csv
        self.path_to_main_csv = path_to_main_csv
        self.chunk_size = chunk_size

    def download_n_files(self):
        with open(self.path_to_main_csv, "r") as f:
            count = 0
            lines = []
            for line in f:
                count += 1
                lines.append(line)
                if count % self.chunk_size == 0:
                    write_chunk(count // self.chunk_size, lines, self.path_to_tmp_csv)
                    lines = []
            if len(lines) > 0:
                write_chunk((count // self.chunk_size) + 1, lines, self.path_to_tmp_csv)

    def get_paths_tmp_csv(self):
        return [self.path_to_tmp_csv + name for name in os.listdir(self.path_to_tmp_csv) if name != ".DS_Store"]

    def delete_tmp_directory(self):
        shutil.rmtree(self.path_to_tmp_csv)


