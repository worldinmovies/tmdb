from celery import shared_task
from channels.layers import get_channel_layer

from app.helper import log
from app.models import Movie, Title, AlternativeTitles
from django.db import transaction


@shared_task
def redo_countries(movie_ids):
    def work(movie: Movie):
        Movie.objects(id=movie.id).update_one(set__guessed_country=movie.guess_country())

    layer = get_channel_layer()
    try:
        with transaction.atomic():
            [work(movie) for movie in Movie.objects(pk__in=movie_ids).only('id',
                                                                           'original_language',
                                                                           'origin_country',
                                                                           'production_countries',
                                                                           'production_companies')]
        log(message=f"Processed {len(movie_ids)} movies, guestimating countries", layer=layer)
    except Exception as e:
        log(message=f"Error handling: {movie_ids} in redo_countries with error: e", layer=layer, e=e)


@shared_task
def import_imdb_ratings_task(csv_rows_chunk):
    def make_entity(db: Movie, csv):
        db.imdb_vote_average = float(csv[1])
        db.imdb_vote_count = int(csv[2])
        db.calculate_weighted_rating_bayes()
        return db

    movies = dict()
    try:
        [movies.setdefault(movie[0], movie) for movie in csv_rows_chunk]

        data = Movie.objects.filter(imdb_id__in=[csv_row[0] for csv_row in csv_rows_chunk])
        with transaction.atomic():
            for movie in [make_entity(d, movies[d.imdb_id]) for d in data]:
                Movie.objects(pk=movie.id).update(set__imdb_vote_average=movie.imdb_vote_average,
                                                  set__imdb_vote_count=movie.imdb_vote_count,
                                                  set__weighted_rating=movie.weighted_rating)
        log(message=f"Processed {len(csv_rows_chunk)} ratings")
    except Exception as e:
        log(message=f"Failed processing ratings for ids: {movies.keys()} due to error: {e}", e=e)


@shared_task
def import_imdb_titles_task(chunk):
    chunked_map = dict()
    try:
        [chunked_map.setdefault(x[0], []).append({"alt_title": x[2], "iso": x[3]}) for x in chunk]
        with transaction.atomic():
            for fetched in Movie.objects.filter(imdb_id__in=list(chunked_map.keys())):
                for alt in chunked_map.get(fetched.imdb_id):
                    iso = alt['iso']
                    title = alt['alt_title']
                    if not fetched.alternative_titles:
                        fetched.alternative_titles = AlternativeTitles()
                    if iso != r'\N' and title not in fetched.alternative_titles.titles:
                        for t in [title for title in fetched.alternative_titles.titles if title.iso_3166_1 == iso and title.type == 'IMDB']:
                            fetched.alternative_titles.titles.remove(t)
                        fetched.alternative_titles.titles.append(Title(iso_3166_1=iso, title=title, type='IMDB'))
                fetched.save()
        log(message=f"Processed {len(chunk)} titles")
    except Exception as e:
        log(message=f"Failed processing ratings for ids: {chunked_map.keys()} due to error: {e}", e=e)
