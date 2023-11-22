from celery import shared_task

from app.helper import __send_data_to_channel
from app.models import FlattenedMovie, Movie, Genre, SpokenLanguage, ProductionCountries, AlternativeTitles, Title
from django.db import transaction
from channels.layers import get_channel_layer


@shared_task
def flattify_movies(json_chunk):
    to_insert = [FlattenedMovie.create(movie_id) for movie_id in
                 Movie.objects(pk__in=json_chunk).values_list('data')]
    FlattenedMovie.objects.insert(to_insert)
    print("Persisted2: %s" % len(to_insert))


@shared_task
def redo_movies_task(movie_ids):
    def persist(movie: Movie):
        Movie.add_references(all_genres, all_langs, all_countries, movie.data)
        movie.save()

    all_genres = dict([(gen.id, gen) for gen in Genre.objects.all()])
    all_langs = dict([(lang.iso_639_1, lang) for lang in SpokenLanguage.objects.all()])
    all_countries = dict([(country.iso_3166_1, country) for country in ProductionCountries.objects.all()])

    ids = [persist(x) for x in Movie.objects(pk__in=movie_ids)]
    print("Processed: %s" % len(ids))


@shared_task
def import_imdb_ratings_task(csv_rows_chunk):
    def make_entity(db, csv):
        db.imdb_vote_average = csv[1]
        db.imdb_vote_count = csv[2]
        db.weighted_rating = db.calculate_weighted_rating_log()

    movies = dict()
    for movie in csv_rows_chunk:
        movies[movie[0]] = movie

    data = Movie.objects.filter(imdb_id__in=[csv_row[0] for csv_row in csv_rows_chunk])
    with transaction.atomic():
        bulk = [make_entity(db, movies[db.imdb_id]) for db in data]
        if bulk:
            FlattenedMovie.objects.bulk_update(bulk, ["imdb_vote_average",
                                                      "imdb_vote_count",
                                                      "weighted_rating"])
    __send_data_to_channel(layer=get_channel_layer(), message=f"Processed {len(csv_rows_chunk)} ratings")


@shared_task
def import_imdb_titles_task(chunk):
    chunked_map = dict()
    [chunked_map.setdefault(x[0], []).append({"alt_title": x[2], "iso": x[3]}) for x in chunk]
    fetched_movies = Movie.objects.filter(imdb_id__in=chunked_map.keys())

    alt_titles = []
    for fetched in fetched_movies:
        for alt in chunked_map.get(fetched.imdb_id):
            iso = alt['iso']
            title = alt['alt_title']
            if iso != r'\N' and not fetched.alternative_titles.filter(title=title).exists():
                fetched.alternative_titles.append(Title(iso_3166_1=iso, title=title, type='IMDB'))
        fetched.save()
    if alt_titles:
        AlternativeTitles.objects.bulk_create(alt_titles)
    __send_data_to_channel(layer=get_channel_layer(), message=f"Processed {len(chunk)} titles")
