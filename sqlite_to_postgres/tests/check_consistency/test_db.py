import os

import pandas as pd
import psycopg2
import pytest
from check_eq_data import download_postgres_data
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture()
def postgres_count():
    with psycopg2.connect(database=os.environ.get('DB_NAME'), user=os.environ.get('DB_USER'),
                          password=os.environ.get('DB_PASSWORD'), host='127.0.0.1', port=5432) as conn:
        with conn.cursor() as curs:
            query_film_work = "SELECT COUNT(*) FROM content.film_work;"
            curs.execute(query_film_work)
            count_1 = curs.fetchone()

            query_person = "SELECT COUNT(*) FROM content.person;"
            curs.execute(query_person)
            count_2 = curs.fetchone()

            query_genre = "SELECT COUNT(*) FROM content.genre;"
            curs.execute(query_genre)
            count_3 = curs.fetchone()

            query_genre_film_work = "SELECT COUNT(*) FROM content.genre_film_work;"
            curs.execute(query_genre_film_work)
            count_4 = curs.fetchone()

            query_person_film_work = "SELECT COUNT(*) FROM content.person_film_work;"
            curs.execute(query_person_film_work)
            count_5 = curs.fetchone()
    conn.close()
    return [count_1[0], count_2[0], count_3[0], count_4[0], count_5[0]]


def test_eq_count(postgres_count):
    path = os.environ.get('PATH_')
    names = ['film_work.csv', 'person.csv', 'genre.csv', 'genre_film_work.csv', 'person_film_work.csv']
    tuples = [pd.read_csv(path + name, sep=';', header=None).shape[0] for name in names]

    assert postgres_count == tuples


def test_capitalize():
    assert download_postgres_data() is True
