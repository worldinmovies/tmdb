import csv

import datetime
import json
import threading

from app.celery_tasks import redo_countries
from app.helper import chunks, convert_country_code, start_background_process
from app.imdb_importer import import_imdb_ratings, import_imdb_alt_titles
from app.tmdb_importer import download_files, fetch_tmdb_data_concurrently, import_genres, import_countries, \
    import_languages, \
    base_import, check_which_movies_needs_update, import_providers
from app.models import Movie, Genre, SpokenLanguage, ProductionCountries
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
    return HttpResponse(json.dumps({"total": 0, "fetched": 0, "percentageDone": 0}))


@csrf_exempt
def get_best_movies_from_country(request, country_code):
    skip = int(request.GET.get('skip', 0))
    limit = int(request.GET.get('limit', 20))
    country_codes = convert_country_code(country_code)
    data = get_movies_from_country_codes(country_codes, limit, skip)
    return HttpResponse(data.to_json(), content_type='application/json')


# Get the best movie from each country until you've gone through all countries,
# then reset the country-list, go through everything again but get the next best film, and so on...
def get_best_randoms(request, movies=0):
    limit = int(request.GET.get('limit', 4))

    no_of_countries = len(Movie.objects.distinct('guessed_country'))
    countries_skip = movies % no_of_countries
    movie_skip = int(movies / no_of_countries)

    print("NO OF COUNTRIES: %s" % no_of_countries)
    movies = Movie.objects.aggregate([
    {"$match": {"guessed_country": {"$ne": None}}},
    {"$group": {
        "_id": "$guessed_country",
        "topMovies": {
            "$topN": {
                "sortBy": {"weighted_rating": -1},
                "output": {
                    "_id": "$_id",
                    "imdb_id": "$imdb_id",
                    "original_title": "$original_title",
                    "overview": "$overview",
                    "poster_path": "$poster_path",
                    "vote_average": "$vote_average",
                    "vote_count": "$vote_count",
                    "imdb_vote_average": "$imdb_vote_average",
                    "imdb_vote_count": "$imdb_vote_count",
                    "guessed_country": "$guessed_country",
                    "credits": "$credits",   # keep credits so we can filter later
                    "year": "$release_date"
                },
                "n": movie_skip + 1
            }
        }
    }},
    {"$sort": {"_id": 1}},
    {"$skip": countries_skip},
    {"$limit": limit},
    {"$project": {"movie": {"$arrayElemAt": ["$topMovies", movie_skip]}}},
    {"$replaceRoot": {"newRoot": "$movie"}},
    # NOW extract director from the limited credits
    {"$addFields": {
        "director": {
            "$first": {
                "$map": {
                    "input": {
                        "$filter": {
                            "input": "$credits.crew",
                            "as": "c",
                            "cond": {"$eq": ["$$c.job", "Director"]}
                        }
                    },
                    "as": "d",
                    "in": "$$d.name"
                }
            }
        }
    }},
    {"$project": {"credits": 0}}
])
    return HttpResponse(json.dumps(list(movies)), content_type='application/json')


def get_movies_from_country_codes(country_codes, limit, skip):
    return (Movie.objects(guessed_country__in=country_codes)
            .order_by('-weighted_rating')
            .limit(limit)
            .skip(skip))


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


def fetch_providers(request):
    return HttpResponse(start_background_process(import_providers, 'import_providers', 'TMDB Providers'))


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
    data_list = Movie.objects(pk__in=map(lambda x: int(x), ids.split(','))).exclude(
        'fetched',
        'fetched_date',
        'data').to_json()
    return HttpResponse(list(data_list), content_type='application/json')


def dump_genres(request):
    return HttpResponse(Genre.objects.all().to_json(), content_type='application/json')


def dump_langs(request):
    return HttpResponse(SpokenLanguage.objects.all().to_json(), content_type='application/json')


def dump_countries(request):
    return HttpResponse(ProductionCountries.objects.all().to_json(), content_type='application/json')


def redo_guestimation(request):
    def work():
        for chunk in chunks(Movie.objects().all().values_list('id'), 50):
            redo_countries.delay(list(chunk))

    return HttpResponse(start_background_process(work, 'guestimate_countries', 'Redoing Guestimation Of Countries'))


@csrf_exempt
def ratings(request):
    """This should map incoming imdb ratings file, and try to match it with our dataset,
        and return it in a format we can use in frontend

        curl 'http://localhost:8000/ratings' -X POST -H
        'Content-Type: multipart/form-data' -F file=@testdata/ratings.csv
    """
    print("Receiving stuff")
    if request.method == 'POST':
        if 'file' in request.FILES:
            file = request.FILES['file']
            csv_as_dicts = csv.DictReader(file.read().decode('utf8').splitlines())
            # Const,Your Rating,Date Rated,Title,URL,Title Type,IMDb Rating,
            # Runtime (mins),Year,Genres,Num Votes,Release Date,Directors
            result = {'found': {}, 'not_found': []}
            data = {}
            for i in [json.loads(json.dumps(x)) for x in csv_as_dicts]:
                data[i['Const']] = {"title": i['Title'], "year": i['Year']}

            count = 0
            for i in chunks(data.items(), 100):
                u = [x[0] for x in i]
                count = count + len(u)
                matches = Movie.objects(imdb_id__in=u).only('guessed_country', 'imdb_id',
                                                            'id', 'original_title',
                                                            'release_date', 'poster_path',
                                                            'vote_average', 'vote_count')
                for match in matches:
                    if match.guessed_country:
                        country = match.guessed_country
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
