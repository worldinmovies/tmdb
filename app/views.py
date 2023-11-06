import datetime
import json
import threading
from mongoengine.queryset.visitor import Q

from app.helper import chunks, convert_country_code, start_background_process
from app.importer import download_files, fetch_tmdb_data_concurrently, import_genres, import_countries, \
    import_languages, \
    base_import, check_which_movies_needs_update
from app.models import Movie, Genre, SpokenLanguage, ProductionCountries
from django.http import HttpResponse
from app.kafka import produce
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
    country_codes = convert_country_code(country_code)
    print("COUNTRYCODES: %s" % country_codes)
    print(list(Movie.objects.filter(Q(fetched=True) & Q(data__production_countries__iso_3166_1__in=country_codes))))
    return Movie.objects.filter(Q(fetched=True) & Q(data__production_countries__iso_3166_1__in=country_codes)).to_json()


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
    data_list = Movie.objects.filter(pk__in=movie_ids).values_list('data')
    return HttpResponse(json.dumps([data for data in data_list]),
                        content_type='application/json')


def dump_genres(request):
    data = [{"id": x.id, "name": x.name} for x in Genre.objects.all()]
    return HttpResponse(json.dumps(data), content_type='application/json')


def dump_langs(request):
    data = [{"iso_639_1": x.iso_639_1, "name": x.name} for x in SpokenLanguage.objects.all()]
    return HttpResponse(json.dumps(data), content_type='application/json')


def dump_countries(request):
    data = [{"iso_3166_1": x.iso_3166_1, "name": x.name} for x in ProductionCountries.objects.all()]
    return HttpResponse(json.dumps(data), content_type='application/json')


def generate_kafka_dump(request):
    def gen():
        for chunk in chunks(Movie.objects.all().values_list('id'), 1000):
            [produce('NEW', x, topic='data_dump') for x in chunk]

    return HttpResponse(start_background_process(gen, 'generate_kafka_dump', 'kafka dump'))
