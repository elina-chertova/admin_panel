import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def download_postgres_data():
    path = os.environ.get('PATH_')
    result = []
    with psycopg2.connect(database=os.environ.get('DB_NAME'), user=os.environ.get('DB_USER'),
                          password=os.environ.get('DB_PASSWORD'), host='127.0.0.1', port=5432) as conn:
        with conn.cursor() as curs:
            query_film_work = "SELECT * FROM content.film_work;"
            curs.execute(query_film_work)
            df_fw_post = pd.DataFrame(curs.fetchall())
            df_fw_post.columns = ['id', 'title', 'description', 'creation_date', 'rating', 'type',
                                  'created', 'modified']
            df_fw_sqlite = pd.read_csv(path + 'film_work.csv', sep=';')
            df_fw_sqlite.columns = ['id', 'title', 'description', 'creation_date', 'file_path', 'rating', 'type',
                                    'created', 'modified']
            result.append(list(df_fw_sqlite['id']).sort() == list(df_fw_post['id']).sort())
            result.append(list(df_fw_sqlite['title']).sort() == list(df_fw_post['title']).sort())
            result.append(list(df_fw_sqlite['rating']).sort() == list(df_fw_post['rating']).sort())
            result.append(list(df_fw_sqlite['type']).sort() == list(df_fw_post['type']).sort())

            query_person = "SELECT * FROM content.person;"
            curs.execute(query_person)
            df_p_post = pd.DataFrame(curs.fetchall())
            df_p_post.columns = ['id', 'full_name', 'created', 'modified']
            df_p_sqlite = pd.read_csv(path + 'person.csv', sep=';')
            df_p_sqlite.columns = ['id', 'full_name', 'created', 'modified']
            result.append(list(df_p_sqlite['id']).sort() == list(df_p_post['id']).sort())
            result.append(list(df_p_sqlite['full_name']).sort() == list(df_p_post['full_name']).sort())

            query_genre = "SELECT * FROM content.genre;"
            curs.execute(query_genre)
            df_g_post = pd.DataFrame(curs.fetchall())
            df_g_post.columns = ['id', 'name', 'description', 'created', 'modified']
            df_g_sqlite = pd.read_csv(path + 'genre.csv', sep=';')
            df_g_sqlite.columns = ['id', 'name', 'description', 'created', 'modified']
            result.append(list(df_g_sqlite['id']).sort() == list(df_g_post['id']).sort())
            result.append(list(df_g_sqlite['name']).sort() == list(df_g_post['name']).sort())

            query_genre_film_work = "SELECT * FROM content.genre_film_work;"
            curs.execute(query_genre_film_work)
            df_gfw_post = pd.DataFrame(curs.fetchall())
            df_gfw_post.columns = ['id', 'film_work_id', 'genre_id', 'created']
            df_gfw_sqlite = pd.read_csv(path + 'genre_film_work.csv', sep=';')
            df_gfw_sqlite.columns = ['id', 'film_work_id', 'genre_id', 'created']
            result.append(list(df_gfw_sqlite['id']).sort() == list(df_gfw_post['id']).sort())
            result.append(list(df_gfw_sqlite['film_work_id']).sort() == list(df_gfw_post['film_work_id']).sort())
            result.append(list(df_gfw_sqlite['genre_id']).sort() == list(df_gfw_post['genre_id']).sort())

            query_person_film_work = "SELECT * FROM content.person_film_work;"
            curs.execute(query_person_film_work)
            df_pfw_post = pd.DataFrame(curs.fetchall())
            df_pfw_post.columns = ['id', 'film_work_id', 'person_id', 'role', 'created']
            df_pfw_sqlite = pd.read_csv(path + 'person_film_work.csv', sep=';')
            df_pfw_sqlite.columns = ['id', 'film_work_id', 'person_id', 'role', 'created']
            result.append(list(df_pfw_sqlite['id']).sort() == list(df_pfw_post['id']).sort())
            result.append(list(df_pfw_sqlite['film_work_id']).sort() == list(df_pfw_post['film_work_id']).sort())
            result.append(list(df_pfw_sqlite['person_id']).sort() == list(df_pfw_post['person_id']).sort())
            result.append(list(df_pfw_sqlite['role']).sort() == list(df_pfw_post['role']).sort())
    conn.close()
    return False not in result
