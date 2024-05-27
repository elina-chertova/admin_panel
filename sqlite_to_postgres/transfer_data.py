import os

from dotenv import load_dotenv
from models_dataclasses import (FilmWork, Genre, GenreFilmWork, Person,
                                PersonFilmWork)
from project_classes import PostgresSaver, PrepareDataset, SQLiteLoader
from settings import database
import dataclasses
load_dotenv()


SQLITE = '/Users/elinachertova/PycharmProjects/new_admin_panel_sprint_1/sqlite_to_postgres/sqlite_db/'
if not os.path.isdir(SQLITE):
    os.mkdir(SQLITE)
FILMWORK_TMP_PATH = SQLITE + 'tmp_filmwork/'
FILMWORK_PATH = SQLITE + 'film_work.csv'


PERSON_TMP_PATH = SQLITE + 'tmp_person/'
PERSON_PATH = SQLITE + 'person.csv'


GENRE_TMP_PATH = SQLITE + 'tmp_genre/'
GENRE_PATH = SQLITE + 'genre.csv'

GENRE_FW_TMP_PATH = SQLITE + 'tmp_genre_fw/'
GENRE_FW_PATH = SQLITE + 'genre_film_work.csv'


PERSON_FW_TMP_PATH = SQLITE + 'tmp_person_fw/'
PERSON_FW_PATH = SQLITE + 'person_film_work.csv'


if __name__ == "__main__":
    table_map_ = {'film_work': FilmWork, 'genre': Genre, 'person': Person,
                  'person_film_work': PersonFilmWork, 'genre_film_work': GenreFilmWork}
    table_tmp = {'film_work': [FILMWORK_TMP_PATH, FILMWORK_PATH],
                 'genre': [GENRE_TMP_PATH, GENRE_PATH],
                 'person': [PERSON_TMP_PATH, PERSON_PATH],
                 'person_film_work': [PERSON_FW_TMP_PATH, PERSON_FW_PATH],
                 'genre_film_work': [GENRE_FW_TMP_PATH, GENRE_FW_PATH]}

    for table_ in table_map_.keys():
        if not os.path.isdir(table_tmp[table_][0]):
            os.mkdir(table_tmp[table_][0])

        sqlloader = SQLiteLoader(table_map_, os.environ.get('SQLITE_DB_NAME'), SQLITE, 1000)
        sqlloader.extract_data_from(table_)

        preparedata = PrepareDataset(table_tmp[table_][0], table_tmp[table_][1], chunk_size=40)
        preparedata.download_n_files()

        table_parts = preparedata.get_paths_tmp_csv()
        postgressaver = PostgresSaver(database, table_, table_map_)
        postgressaver.insert_n_data_to_db(table_parts)

        preparedata.delete_tmp_directory()
