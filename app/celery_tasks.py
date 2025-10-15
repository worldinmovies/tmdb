from celery import shared_task
from channels.layers import get_channel_layer

from app.helper import log
from app.models import Movie, Title, AlternativeTitles, DiscoveryMovie
from django.db import transaction
from app.meilisearch_client import client
# app/tasks.py



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
                        for t in [title for title in fetched.alternative_titles.titles if
                                  title.iso_3166_1 == iso and title.type == 'IMDB']:
                            fetched.alternative_titles.titles.remove(t)
                        fetched.alternative_titles.titles.append(Title(iso_3166_1=iso, title=title, type='IMDB'))
                fetched.save()
        log(message=f"Processed {len(chunk)} titles")
    except Exception as e:
        log(message=f"Failed processing ratings for ids: {chunked_map.keys()} due to error: {e}", e=e)


@shared_task
def populate_discovery_movie_task(chunk):
    """
    Process a chunk of movie IDs and populate DiscoveryMovie collection
    """
    try:
        movies = Movie.objects.filter(
            id__in=chunk
        ).only(
            'id',
            'imdb_id',
            'original_title',
            'title',
            'poster_path',
            'vote_average',
            'vote_count',
            'imdb_vote_average',
            'imdb_vote_count',
            'guessed_country',
            'release_date',
            'credits',
            'genres',
            'weighted_rating',
            'overview',
            'fetched',
            'guessed_country'
        )
        
        discovery_movies = []
        
        for movie in [ movie for movie in movies if movie.fetched and movie.guessed_country ]:
            # Extract director from credits
            director = None
            if movie.credits and movie.credits.crew:
                directors = [
                    crew.name for crew in movie.credits.crew 
                    if crew.job == "Director"
                ]
                director = directors[0] if directors else None
            
            # Extract year from release_date
            year = None
            if movie.release_date:
                try:
                    year = movie.release_date.split('-')[0]
                except (AttributeError, IndexError):
                    pass
            
            # Get genre names
            genre_names = [genre.name for genre in movie.genres] if movie.genres else []
            
            english_title = movie.title if movie.title else ""
            
            # Create or update DiscoveryMovie
            discovery_movie = DiscoveryMovie(
                id=movie.id,
                imdb_id=movie.imdb_id,
                original_title=movie.original_title,
                english_title=english_title,
                poster_path=movie.poster_path,
                vote_average=movie.vote_average if movie.vote_average else 0.0,
                vote_count=movie.vote_count if movie.vote_count else 0,
                estimated_country=movie.guessed_country,
                year=year,
                director=director,
                genres=genre_names,
                weighted_rating=movie.weighted_rating if movie.weighted_rating else 0.0,
                overview=movie.overview
            )
            
            discovery_movies.append(discovery_movie)
        
        # Bulk save with upsert
        for dm in discovery_movies:
            dm.save()
        
        log(message=f"Processed {len(discovery_movies)} movies into DiscoveryMovie collection")
        return len(discovery_movies)
        
    except Exception as e:
        error_msg = f"Failed processing discovery movies for chunk due to error: {e}"
        log(message=error_msg, e=e)
        raise



def extract_directors(crew):
    return [member.name for member in crew if member.job == "Director"]

def extract_alternative_titles(alt_titles):
    if not alt_titles:
        return []
    return [title.title for title in alt_titles.titles if title.title]


@shared_task
def index_movies(chunk):
    index = client.index("movies")
    try:
        movies = Movie.objects.filter(
                id__in=chunk
            )
        documents = [
            {
            "id": movie.id,
            "title": movie.title,
            "original_title": movie.original_title,
            "alternative_titles": extract_alternative_titles(movie.alternative_titles),
            "overview": movie.overview,
            "directors": extract_directors(movie.credits.crew) if movie.credits else [],
            "weighted_rating": movie.weighted_rating,
            "vote_average": (movie.vote_average + movie.imdb_vote_average) / 2,
            "vote_count": movie.vote_count + movie.imdb_vote_count,
            "guessed_country": movie.guessed_country,
            "original_language": movie.original_language,
            "poster": movie.poster_path,
            "year": movie.release_date
        }
            for movie in movies
        ]
        index.add_documents(documents)
        log(message=f"Indexed {len(chunk)} movies")
    except Exception as e:
        log(message="Failed to index due to error: %s" % e, e=e)

