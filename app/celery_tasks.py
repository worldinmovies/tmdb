
from celery import shared_task
from channels.layers import get_channel_layer

from settings.celery import app
from app.helper import log, get_statics
from app.models import Movie, Title, AlternativeTitles, MovieDetails
from django.db import transaction


@shared_task
def flattify_movies(movie_ids):
    def work(movie: Movie):
        data: MovieDetails = movie.data
        daat = {"backdrop_path": data.backdrop_path,
                "belongs_to_collection": {"id": data.belongs_to_collection.id,
                                          "name": data.belongs_to_collection.name,
                                          "poster_path": data.belongs_to_collection.poster_path,
                                          "backdrop_path": data.belongs_to_collection.backdrop_path}
                if data.belongs_to_collection else None,
                "budget": data.budget,
                "homepage": data.homepage,
                "imdb_id": data.imdb_id,
                "original_language": data.original_language,
                "original_title": data.original_title,
                "overview": data.overview,
                "popularity": data.popularity,
                "poster_path": data.poster_path,
                "production_countries": [{"iso_3166_1": x.iso_3166_1,
                                          "name": x.name} for x in
                                         data.production_countries] if data.production_countries else [],
                "spoken_languages": [{"iso_639_1": x.iso_639_1,
                                      "name": x.name} for x in data.spoken_languages] if data.spoken_languages else [],
                "genres": [{"id": x.id,
                            "name": x.name} for x in data.genres] if data.genres else [],
                "production_companies": [{"id": x.id,
                                          "logo_path": x.logo_path,
                                          "name": x.name,
                                          "origin_country": x.origin_country} for x in
                                         data.production_companies] if data.production_companies else [],
                "release_date": data.release_date,
                "revenue": data.revenue,
                "runtime": data.runtime,
                "status": data.status,
                "tagline": data.tagline,
                "title": data.title,
                "vote_average": data.vote_average,
                "vote_count": data.vote_count,
                "alternative_titles": {"titles": [{"iso_3166_1": x.iso_3166_1,
                                                   "title": x.title,
                                                   "type": x.type} for x in
                                                  data.alternative_titles.titles]} if data.alternative_titles else None,
                "credits": {"cast": [{"cast_id": x.cast_id,
                                      "character": x.character,
                                      "credit_id": x.credit_id,
                                      "gender": x.gender,
                                      "id": x.id,
                                      "name": x.name,
                                      "order": x.order,
                                      "profile_path": x.profile_path,
                                      "popularity": x.popularity,
                                      "original_name": x.original_name,
                                      "known_for_department": x.known_for_department
                                      } for x in data.credits.cast],
                            "crew": [{"credit_id": x.credit_id,
                                      "department": x.department,
                                      "gender": x.gender,
                                      "id": x.id,
                                      "job": x.job,
                                      "name": x.name,
                                      "profile_path": x.profile_path,
                                      "popularity": x.popularity,
                                      "original_name": x.original_name,
                                      "known_for_department": x.known_for_department} for x in
                                     data.credits.crew]} if data.credits else None,
                "images": {"backdrops": data.images.backdrops,
                           "posters": data.images.posters,
                           "logos": data.images.logos} if data.images else None
                }
        movie.add_fetched_info(daat, all_genres, all_langs, all_countries)
        movie.data = None
        movie.save()

    layer = get_channel_layer()
    try:
        all_genres, all_langs, all_countries = get_statics()

        with transaction.atomic():
            [work(movie) for movie in Movie.objects(pk__in=movie_ids).all()]
        log(f"Processed {len(movie_ids)} movies into new structure - {app.active} items in queue left", layer=layer)
    except Exception as e:
        log(message=f"Error handling: {movie_ids} with error: e", layer=layer, e=e)


@shared_task
def import_imdb_ratings_task(csv_rows_chunk):
    def make_entity(db: Movie, csv):
        db.imdb_vote_average = float(csv[1])
        db.imdb_vote_count = float(csv[2])
        db.calculate_weighted_rating_bayes()
        return db

    movies = dict()
    try:
        [movies.setdefault(movie[0], movie) for movie in csv_rows_chunk]

        data = Movie.objects.filter(imdb_id__in=[csv_row[0] for csv_row in csv_rows_chunk])
        with transaction.atomic():
            for movie in [make_entity(d, movies[d.imdb_id]) for d in data]:
                Movie.objects(id=movie.id).update(set__imdb_vote_average=movie.imdb_vote_average,
                                                  set__imdb_vote_count=movie.imdb_vote_count,
                                                  set__weighted_rating=movie.weighted_rating)
        log(message=f"Processed {len(csv_rows_chunk)} ratings - {app.active} items in queue left")
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
                        fetched.alternative_titles.titles.append(Title(iso_3166_1=iso, title=title, type='IMDB'))
                fetched.save()
        log(message=f"Processed {len(chunk)} titles - {app.active} items in queue left")
    except Exception as e:
        log(message=f"Failed processing ratings for ids: {chunked_map.keys()} due to error: {e}", e=e)
