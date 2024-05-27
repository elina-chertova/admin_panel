import datetime
import uuid
from dataclasses import dataclass


@dataclass
class FilmWork:
    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime.datetime
    file_path: str
    rating: float
    type: str
    created: datetime.datetime
    modified: datetime.datetime

    @classmethod
    def row_factory(cls, cursor, row):
        return cls(*row)


@dataclass
class Person:
    id: uuid.UUID
    full_name: str
    created: datetime.datetime
    modified: datetime.datetime

    @classmethod
    def row_factory(cls, cursor, row):
        return cls(*row)


@dataclass
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created: datetime.datetime
    modified: datetime.datetime

    @classmethod
    def row_factory(cls, cursor, row):
        return cls(*row)


@dataclass
class GenreFilmWork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: datetime.datetime

    @classmethod
    def row_factory(cls, cursor, row):
        return cls(*row)


@dataclass
class PersonFilmWork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created: datetime.datetime

    @classmethod
    def row_factory(cls, cursor, row):
        return cls(*row)
