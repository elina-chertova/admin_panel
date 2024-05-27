from admin_numeric_filter.admin import RangeNumericFilter
from django.contrib import admin
from rangefilter.filter import DateRangeFilter

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'created', 'modified')
    list_filter = ('name',)
    ordering = ['name']


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'created', 'modified')
    search_fields = ('full_name',)


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline,)

    list_display = ('title', 'type', 'creation_date', 'rating', 'created', 'modified')
    list_filter = ('type', ('creation_date', DateRangeFilter), ('rating', RangeNumericFilter))
    search_fields = ('title', 'description', 'id')
