import csv

import decimal

import datetime
import json
import threading

from babel.languages import get_official_languages
from app.helper import chunks, convert_country_code, start_background_process
from app.imdb_importer import import_imdb_ratings, import_imdb_alt_titles
from app.tmdb_importer import download_files, fetch_tmdb_data_concurrently, import_genres, import_countries, \
    import_languages, \
    base_import, check_which_movies_needs_update
from app.models import Movie, Genre, SpokenLanguage, ProductionCountries, FlattenedMovie
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt


def import_status(request):
    result = Movie.objects().aggregate([
        {
            '$group': {
                '_id': None,
                'Total': {
                    '$sum': 1
                },
                'Fetched': {
                    '$sum': {
                        '$cond': {
                            'if': '$fetched', 'then': 1, 'else': 0
                        }
                    }
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'Total': 1,
                'Fetched': 1,
                'Percentage': {
                    '$multiply': [
                        {'$divide': ['$Fetched', '$Total']},
                        100
                    ]
                }
            }
        }
    ])
    for row in result:
        return HttpResponse(json.dumps({"total": row['Total'],
                                        "fetched": row['Fetched'],
                                        "percentageDone": row['Percentage']}),
                            content_type='application/json')


@csrf_exempt
def get_best_movies_from_country(request, country_code):
    skip = int(request.GET.get('skip', 0))
    limit = int(request.GET.get('limit', 20))
    country_codes = convert_country_code(country_code)
    data = (FlattenedMovie.objects(guessed_countries__in=country_codes)
            .order_by('-weighted_rating')
            .limit(limit)
            .skip(skip))
    return HttpResponse(data.to_json(), content_type='application/json')


# Imports


def download_file(request):
    return HttpResponse(start_background_process(download_files, 'download_files', 'TMDB downloads'))


def base_fetch(request):
    return HttpResponse(start_background_process(base_import, 'base_import', 'TMDB base'))


def import_tmdb_data(request):
    return HttpResponse(start_background_process(fetch_tmdb_data_concurrently, 'import_tmdb_data', 'TMDB data'))


def fetch_genres(request):
    return HttpResponse(start_background_process(import_genres, 'import_genres', 'TMDB genres'))


def fetch_countries(request):
    return HttpResponse(start_background_process(import_countries, 'import_countries', 'TMDB countries'))


def fetch_languages(request):
    return HttpResponse(start_background_process(import_languages, 'import_languages', 'TMDB languages'))


def check_tmdb_for_changes(request):
    start_date = request.GET.get('start_date',
                                 (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    end_date = request.GET.get('end_date', datetime.date.today().strftime("%Y-%m-%d"))
    if 'check_which_movies_needs_update' not in [thread.name for thread in threading.enumerate()]:
        thread = threading.Thread(target=check_which_movies_needs_update,
                                  args=[start_date, end_date],
                                  name='check_which_movies_needs_update')
        thread.daemon = True
        thread.start()
        return HttpResponse(json.dumps({"Message": "Starting to process TMDB changes"}))
    else:
        return HttpResponse(json.dumps({"Message": "TMDB changes process already started"}))


def fetch_movie_data(request, ids):
    movie_ids = list(map(lambda x: int(x), ids.split(',')))
    data_list = FlattenedMovie.objects(pk__in=movie_ids).to_json()
    return HttpResponse(data_list, content_type='application/json')


def dump_genres(request):
    return HttpResponse(Genre.objects.all().to_json(), content_type='application/json')


def dump_langs(request):
    return HttpResponse(SpokenLanguage.objects.all().to_json(), content_type='application/json')


def dump_countries(request):
    return HttpResponse(ProductionCountries.objects.all().to_json(), content_type='application/json')


def redo_movies(request):
    def work():
        all_genres = dict([(gen.id, gen) for gen in Genre.objects.all()])
        all_langs = dict([(lang.iso_639_1, lang) for lang in SpokenLanguage.objects.all()])
        all_countries = dict([(country.iso_3166_1, country) for country in ProductionCountries.objects.all()])

        ids = Movie.objects.all()
        count = 0
        for chunk in chunks(ids, 100):
            for movie in chunk:
                try:
                    Movie.add_references(all_genres, all_langs, all_countries, movie.data)
                    movie.save()
                except Exception as e:
                    print("Could not persist: %s due to: %s" % (movie.id, e))
                count += 1
            print("Persisted: %s" % count)

    return HttpResponse(start_background_process(work, 'redo_persistence', 'Redoing Persistence'))


def create_flattened_structure(request):
    def work():
        movies = Movie.objects.all()
        count = 0
        FlattenedMovie.objects().all().delete()
        for chunk in chunks(movies, 100):
            to_insert = [FlattenedMovie(id=movie.id,
                                        backdrop_path=movie.data.backdrop_path,
                                        belongs_to_collection=movie.data.belongs_to_collection,
                                        budget=movie.data.budget,
                                        genres=[x.name for x in movie.data.genres],
                                        homepage=movie.data.homepage,
                                        imdb_id=movie.data.imdb_id,
                                        original_language=movie.data.original_language,
                                        original_title=movie.data.original_title,
                                        overview=movie.data.overview,
                                        popularity=movie.data.popularity,
                                        poster_path=movie.data.poster_path,
                                        production_companies=movie.data.production_companies,
                                        production_countries=[{"iso": x.iso_3166_1, "name": x.name} for x in
                                                              movie.data.production_countries],
                                        release_date=movie.data.release_date,
                                        revenue=movie.data.revenue,
                                        runtime=movie.data.runtime,
                                        spoken_languages=[{"iso": x.iso_639_1, "name": x.name} for x in
                                                          movie.data.spoken_languages],
                                        status=movie.data.status,
                                        tagline=movie.data.tagline,
                                        title=movie.data.title,
                                        vote_average=movie.data.vote_average,
                                        vote_count=movie.data.vote_count,
                                        alternative_titles=movie.data.alternative_titles,
                                        credits=movie.data.credits,
                                        external_ids=movie.data.external_ids,
                                        images=movie.data.images,
                                        weighted_rating=calculate_weighted_rating_bayes(movie.data),
                                        guessed_countries=guess_countries(movie.data)) for movie in chunk]
            FlattenedMovie.objects.insert(to_insert)
            count += len(to_insert)
            print("Persisted: %s" % count)

    return HttpResponse(start_background_process(work, 'flattify_movies', 'Redoing Persistence'))


def calculate_weighted_rating_bayes(movie):
    """
    The formula for calculating the Top Rated 250 Titles gives a true Bayesian estimate:
    weighted rating (WR) = (v ÷ (v+m)) × R + (m ÷ (v+m)) × C where:

    R = average for the movie (mean) = (Rating)
    v = number of votes for the movie = (votes)
    m = minimum votes required to be listed in the Top 250 (currently 25000)
    C = the mean vote across the whole report (currently 7.0)
    """

    v = decimal.Decimal(movie.vote_count) + \
        decimal.Decimal(movie.imdb_vote_count)
    m = decimal.Decimal(200)
    r = decimal.Decimal(movie.vote_average) + \
        decimal.Decimal(movie.imdb_vote_average)
    c = decimal.Decimal(4)
    return (v / (v + m)) * r + (m / (v + m)) * c


def guess_countries(movie):
    orig_lang = movie.original_language
    production_countries = [x.iso_3166_1 for x in movie.production_countries if x]

    for country in [country for country in production_countries]:
        official_langs = get_official_languages(territory=country, de_facto=True, regional=True)
        if orig_lang in official_langs:
            return [country]
    if production_countries:
        return [production_countries[0]]
    else:
        return []


@csrf_exempt
def ratings(request):
    """This should map incoming imdb ratings file, and try to match it with our dataset,
        and return it in a format we can use in frontend

        curl 'http://localhost:8000/ratings' -X POST -H 'Content-Type: multipart/form-data' -F file=@testdata/ratings.csv
    """
    print("Receiving stuff")
    if request.method == 'POST':
        if 'file' in request.FILES:
            file = request.FILES['file']
            csv_as_dicts = csv.DictReader(file.read().decode('utf8').splitlines())
            # Const,Your Rating,Date Rated,Title,URL,Title Type,IMDb Rating,Runtime (mins),Year,Genres,Num Votes,Release Date,Directors
            result = {'found': {}, 'not_found': []}
            data = {}
            for i in [json.loads(json.dumps(x)) for x in csv_as_dicts]:
                data[i['Const']] = {"title": i['Title'], "year": i['Year']}

            count = 0
            for i in chunks(data.items(), 100):
                u = [x[0] for x in i]
                count = count + len(u)
                matches = FlattenedMovie.objects(imdb_id__in=u).all()
                for match in matches:
                    if match.guessed_countries:
                        country = match.guessed_countries[0]
                        result['found'].setdefault(country, []).append({
                            'imdb_id': match.imdb_id,
                            'id': match.id,
                            'original_title': match.original_title,
                            'release_date': match.release_date,
                            'poster_path': match.poster_path,
                            'vote_average': match.vote_average,
                            'vote_count': match.vote_count,
                            'country_code': country
                        })
                print("Processed: %s" % count)

            return HttpResponse(json.dumps(result), content_type='application/json')

    return HttpResponse("Method: %s, not allowed" % request.method, status=400)


# Imports
def fetch_imdb_ratings(request):
    return HttpResponse(start_background_process(import_imdb_ratings, 'import_imdb_ratings', 'IMDB ratings'))


def fetch_imdb_titles(request):
    return HttpResponse(start_background_process(import_imdb_alt_titles, 'import_imdb_titles', 'IMDB titles'))
