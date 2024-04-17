from celery import shared_task

from app.helper import log
from app.models import FlattenedMovie, Movie, Genre, SpokenLanguage, ProductionCountries, Title, AlternativeTitles
from django.db import transaction


@shared_task
def flattify_movies(json_chunk):
    to_insert = [FlattenedMovie.create(movie_id) for movie_id in
                 Movie.objects(pk__in=json_chunk).values_list('data')]
    with transaction.atomic():
        FlattenedMovie.objects.insert(to_insert)
    print("Persisted %s flattened movies" % len(to_insert))


@shared_task
def redo_movies_task(movie_ids):
    def persist(movie: Movie):
        Movie.add_references(all_genres, all_langs, all_countries, dict(movie.data))
        movie.save()

    all_genres = dict[Genre]([(gen.id, gen) for gen in Genre.objects.all()])
    all_langs = dict[SpokenLanguage]([(lang.iso_639_1, lang) for lang in SpokenLanguage.objects.all()])
    all_countries = dict[ProductionCountries](
        [(country.iso_3166_1, country) for country in ProductionCountries.objects.all()])

    with transaction.atomic():
        ids = [persist(x) for x in Movie.objects(pk__in=movie_ids)]
        log(f"Redone {len(ids)} movies with new structure")


@shared_task
def import_imdb_ratings_task(csv_rows_chunk):
    def make_entity(db, csv):
        db.data.imdb_vote_average = float(csv[1])
        db.data.imdb_vote_count = float(csv[2])
        db.data.weighted_rating = float(FlattenedMovie.calculate_weighted_rating_bayes(db.data))
        return db

    movies = dict()
    try:
        for movie in csv_rows_chunk:
            movies[movie[0]] = movie

        data = Movie.objects.filter(data__imdb_id__in=[csv_row[0] for csv_row in csv_rows_chunk])

        with transaction.atomic():
            for movie in [make_entity(d, movies[d.data.imdb_id]) for d in data]:
                Movie.objects(id=movie.id).update(set__data__imdb_vote_average=movie.data.imdb_vote_average,
                                                  set__data__imdb_vote_count=movie.data.imdb_vote_count,
                                                  set__data__weighted_rating=movie.data.weighted_rating)

    except Exception as e:
        log(message=f"Failed processing ratings for ids: {movies.keys()} due to error: {e}", e=e)

    log(message=f"Processed {len(csv_rows_chunk)} ratings")


@shared_task
def import_imdb_titles_task(chunk):
    chunked_map = dict()
    try:
        [chunked_map.setdefault(x[0], []).append({"alt_title": x[2], "iso": x[3]}) for x in chunk]
        with transaction.atomic():
            for fetched in Movie.objects.filter(data__imdb_id__in=list(chunked_map.keys())):
                for alt in chunked_map.get(fetched.data.imdb_id):
                    iso = alt['iso']
                    title = alt['alt_title']
                    if not fetched.data.alternative_titles:
                        fetched.data.alternative_titles = AlternativeTitles()
                    if iso != r'\N' and title not in fetched.data.alternative_titles.titles:
                        fetched.data.alternative_titles.titles.append(Title(iso_3166_1=iso, title=title, type='IMDB'))
                fetched.save()
    except Exception as e:
        log(message=f"Failed processing ratings for ids: {chunked_map.keys()} due to error: {e}", e=e)
    log(message=f"Processed {len(chunk)} titles")
