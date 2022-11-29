from django.contrib import admin
from .models import Genre, Filmwork, GenreFilmwork, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline,)

    list_display = ('title', 'type', 'creation_date', 'rating', 'created_at', 'updated_at',)
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')
