import concurrent.futures
import datetime
import json
import os
import requests
import time
from channels.layers import get_channel_layer
from django.db import transaction
from itertools import chain, islice
from mongoengine import DoesNotExist
from requests.adapters import HTTPAdapter
import sentry_sdk
from urllib3.util.retry import Retry
from app.celery_tasks import populate_discovery_movie_task

from app.helper import __send_data_to_channel, __log_progress, __unzip_file, log, get_statics
from app.models import Movie, SpokenLanguage, Genre, ProductionCountries, WatchProvider


@sentry_sdk.monitor(monitor_slug='base_import')
def base_import():
    download_files()
    import_genres()
    import_countries()
    import_languages()
    import_providers()
    __send_data_to_channel("Base import is done")


def download_files():
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    yesterday_formatted = yesterday.strftime("%m_%d_%Y")
    daily_export_url = "http://files.tmdb.org/p/exports/movie_ids_%s.json.gz" % yesterday_formatted
    response = requests.get(daily_export_url)

    layer = get_channel_layer()
    if response.status_code == 200:
        print("Downloading file")
        with open('movies.json.gz', 'wb') as f:
            f.write(response.content)
        log(layer=layer, message=f"Downloaded {daily_export_url}")

        movies_to_add = []
        tmdb_movie_ids = set()
        contents = __unzip_file('movies.json.gz')
        count = 0
        for b in chunks(contents, 100):
            chunk = []
            u = list(b)
            for i in u:
                try:
                    data = json.loads(i)
                    if data['video'] is False and data['adult'] is False:
                        movie_id = data['id']
                        tmdb_movie_ids.add(movie_id)
                        chunk.append(movie_id)
                except Exception as e:
                    log(layer, f"This line fucked up: {i}, because of {e}", e)
                    print("This line fucked up: %s, because of %s" % (i, e))
            matches = []
            for x in Movie.objects.filter(pk__in=chunk).values_list('id'):
                matches.append(x)
            new_movies = (set(chunk).difference(matches))
            for c in new_movies:
                movies_to_add.append(Movie(id=c, fetched=False))
            count += len(u)
            log(layer=layer, message=f"Parsed {count} out of {len(contents)} movies from downloaded file")

        a = len(movies_to_add)
        log(layer=layer, message=f"{a} movies will be persisted")
        all_unfetched_movie_ids = Movie.objects.filter(fetched=False).all().values_list('id')
        movie_ids_to_delete = (set(all_unfetched_movie_ids).difference(tmdb_movie_ids))
        b = 0
        try:
            log(layer=layer, message=f"Persisting {a} movies")
            for chunk in chunks(movies_to_add, 100):
                to_persist = list(chunk)
                b += len(to_persist)
                Movie.objects.insert(to_persist)
                log(layer=layer, message=f"Persisted {b} movies out of {a}")
            log(layer=layer, message=f"Deleting {len(movie_ids_to_delete)} unfetched movies not in tmdb anymore")
            c = 0
            for movie_to_delete in movie_ids_to_delete:
                Movie.objects.get(pk=movie_to_delete).delete()
                c += 1
                log(layer=layer, message=f"Deleted {c} movies out of {len(movie_ids_to_delete)}")
        except Exception as e:
            print("Error: %s" % e)
            log(layer=layer, message=f"Error persisting or deleting data: {e}", e=e)
    else:
        log(layer=layer, message=f"Error downloading files: {response.status_code} - {response.content}")


def __fetch_movie_with_id(movie_id, index):
    api_key = os.getenv('TMDB_API', 'test')
    url = (f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&language=en-US"
           f"&append_to_response=alternative_titles,credits,external_ids,images,account_states,"
           f"recommendations,watch/providers")
    log(f"Calling url: {url}")
    try:
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=2)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        response = session.get(url, timeout=10)
    except requests.exceptions.Timeout as exc:
        log(f"Timed out on id: {movie_id}... trying again in 10 seconds\n{exc}")
        time.sleep(10)
        return __fetch_movie_with_id(movie_id, index)
    except requests.exceptions.ConnectionError as exc:
        log(f"ConnectionError: {exc} on url: {url}\n Trying again in 10 seconds...")
        time.sleep(30)
        return __fetch_movie_with_id(movie_id, index)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429 or response.status_code == 25:
        retry_after = int(response.headers['Retry-After']) + 1
        time.sleep(retry_after)
        return __fetch_movie_with_id(movie_id, index)
    elif response.status_code == 404:
        Movie.objects.get(pk=movie_id).delete()
        log(f"Deleting movie with id: {movie_id} as it's not in tmdb anymore")
        return None
    elif response.status_code == 401:
        log(f"Unauthorized API key when calling url: {url}")
        raise Exception(f"Unauthorized API key when calling url: {url}")
    else:
        log(f"What is going on?: id:{movie_id}, status:{response.status_code}, response: {response.content}")
        raise Exception("Response: %s, Content: %s" % (response.status_code, response.content))


@sentry_sdk.monitor(monitor_slug='fetch_tmdb_data_concurrently')
def fetch_tmdb_data_concurrently():
    movie_ids = Movie.objects.filter(fetched__exact=False).values_list('id')
    length = len(movie_ids)
    if not length or length == 0:
        log("No new movies to import. Going back to sleep")
        return
    log(f"Starting import of {length} unfetched movies")
    all_genres, all_langs, all_countries = get_statics()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = (executor.submit(__fetch_movie_with_id, movie_id, index) for index, movie_id
                         in enumerate(movie_ids))
        i = 0
        for future in __log_progress(concurrent.futures.as_completed(future_to_url), "TMDB Fetch", length=length):
            try:
                data = future.result()
                if data is not None:
                    movie = Movie.objects.get(pk=data['id'])
                    movie.add_fetched_info(dict(data), all_genres, all_langs, all_countries)
                    movie.save()
                i += 1
            except Exception as exc:
                log(message=f"Could not process data: {exc}", e=exc)


def import_genres():
    log("Importing genres")
    api_key = os.getenv('TMDB_API', 'test')
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={api_key}&language=en-US"
    response = requests.get(url, stream=True)
    layer = get_channel_layer()
    if response.status_code == 200:
        genres_from_json = json.loads(response.content)['genres']
        length = len(genres_from_json)
        i = 0
        all_persisted = Genre.objects.all().values_list('id')
        with transaction.atomic():
            for genre in list(filter(lambda x: x['id'] not in all_persisted, genres_from_json)):
                i += 1
                Genre(id=genre['id'], name=genre['name']).save()
        log(layer=layer, message=f"Fetched {length} genres")
    else:
        log(layer=layer, message=f"Error importing countries: {response.status_code} - {response.content}")


def import_countries():
    print("Importing countries")
    api_key = os.getenv('TMDB_API', 'test')
    url = f"https://api.themoviedb.org/3/configuration/countries?api_key={api_key}"
    response = requests.get(url, stream=True)
    layer = get_channel_layer()
    if response.status_code == 200:
        countries_from_json = json.loads(response.content)
        length = len(countries_from_json)
        i = 0
        all_persisted = ProductionCountries.objects.all().values_list('iso_3166_1')
        with transaction.atomic():
            for country in list(filter(lambda x: x['iso_3166_1'] not in all_persisted, countries_from_json)):
                i += 1
                ProductionCountries(iso_3166_1=country['iso_3166_1'], name=country['english_name']).save()
        log(layer=layer, message=f"Fetched {length} countries")
    else:
        log(layer=layer, message=f"Error importing countries: {response.status_code} - {response.content}")


def import_languages():
    log("Importing languages")
    api_key = os.getenv('TMDB_API', 'test')
    url = f"https://api.themoviedb.org/3/configuration/languages?api_key={api_key}"
    response = requests.get(url, stream=True)
    layer = get_channel_layer()
    if response.status_code == 200:
        languages_from_json = json.loads(response.content)
        length = len(languages_from_json)
        i = 0
        all_persisted = SpokenLanguage.objects.all().values_list('iso_639_1')
        with transaction.atomic():
            for language in list(filter(lambda x: x['iso_639_1'] not in all_persisted, languages_from_json)):
                i += 1
                SpokenLanguage(iso_639_1=language['iso_639_1'], name=language['english_name']).save()
        log(layer=layer, message=f"Fetched {length} languages")
    else:
        log(f"Error importing languages: {response.status_code} - {response.content}")



def populate_discovery_movies():
    """
    Main function to populate the DiscoveryMovie collection from Movie collection.
    Processes movies in chunks and delegates to Celery workers.
    """
    layer = get_channel_layer()
    log(layer=layer, message="Starting DiscoveryMovie population")
    
    # Get all movie IDs that should be in DiscoveryMovie
    qs = Movie.objects.filter(
        fetched=True,
        guessed_country__ne=None
    ).only('id')

    # Get total count without materializing all docs
    total_movies = qs.count()
    log(layer=layer, message=f"Found {total_movies} movies to process")

    processed_count = 0
    chunk_size = 500

    # Iterate lazily in chunks
    for chunk in chunks(qs.scalar('id'), chunk_size):
        chunk_list = list(chunk)
        populate_discovery_movie_task.delay(chunk_list)
        processed_count += len(chunk_list)

        if processed_count % 1000 == 0:
            log(layer=layer, message=f"Queued {processed_count}/{total_movies} movies")

    log(layer=layer, message=f"Finished queuing {total_movies} movies for processing")
    print("Done - all tasks queued")

def import_providers():
    log("Importing providers")
    api_key = os.getenv('TMDB_API', 'test')
    url = f"https://api.themoviedb.org/3/watch/providers/movie?language=en-US?api_key={api_key}"
    response = requests.get(url, stream=True)
    layer = get_channel_layer()
    if response.status_code == 200:
        providers_from_json = json.loads(response.content)['results']
        length = len(providers_from_json)
        i = 0
        all_persisted = WatchProvider.objects.all().values_list('provider_id')
        with transaction.atomic():
            for provider in (x for x in providers_from_json if x['provider_id'] not in all_persisted):
                i += 1
                WatchProvider(**provider).save()
        log(layer=layer, message=f"Fetched {length} providers")
    else:
        log(f"Error importing providers: {response.status_code} - {response.content}")


def chunks(iterable, size=100):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def check_which_movies_needs_update(start_date, end_date):
    """
    :param start_date: Defaults to yesterday
    :param end_date: Defaults to today
    """
    api_key = os.getenv('TMDB_API', 'test')
    page = 1
    url = (f"https://api.themoviedb.org/3/movie/changes?api_key={api_key}&"
           f"start_date={start_date}&end_date={end_date}&page={page}")
    response = requests.get(url, stream=True)
    layer = get_channel_layer()
    if response.status_code == 200:
        data = json.loads(response.content)
        for movie in __log_progress(data['results'], "TMDB Changes"):
            if not movie['adult'] and movie['id']:
                try:
                    db = Movie.objects.get(pk=movie['id'])
                    if db.fetched and db.fetched_date.strftime("%Y-%m-%d") < end_date:
                        Movie.objects.filter(pk=movie['id']).update(fetched=False)
                        log("Scheduling movieId:%s for update" % movie['id'], layer=layer)
                    else:
                        log(layer=layer, message=f"MovieId: {movie['id']} has already been scheduled for update")
                except DoesNotExist:
                    Movie(id=movie['id'], fetched=False).save()
    else:
        log(f"Response: {response.status_code}:{response.content}")


@sentry_sdk.monitor(monitor_slug='cron_endpoint_for_checking_updateable_movies')
def cron_endpoint_for_checking_updateable_movies():
    start_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = (datetime.date.today()).strftime("%Y-%m-%d")
    check_which_movies_needs_update(start_date, end_date)
