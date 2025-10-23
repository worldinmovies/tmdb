import csv
import json
import requests
import sys

from sentry_sdk.crons import monitor
from channels.layers import get_channel_layer

from apps.worker.celery_tasks import import_imdb_ratings_task, import_imdb_titles_task
from apps.app.helper import chunks, __unzip_file, log
from apps.app.db_models import Log, Movie


def parse_user_watched(file):
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
    return result



def import_imdb_ratings():
    """Data-dump of imdbs ratings of all films
       TSV Headers are: tconst, averageRating, numVotes
       and file is about 1 million rows, which takes awhile to process...
       While we only have around 450k rows in our database.
    """
    url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    response = requests.get(url, timeout=300)
    layer = get_channel_layer()
    log(layer=layer, message=f"Downloading file: {url}")
    if response.status_code == 200:
        with open('title.ratings.tsv.gz', 'wb') as f:
            f.write(response.content)
        contents = __unzip_file('title.ratings.tsv.gz')

        reader = csv.reader(contents, delimiter='\t')
        next(reader)
        for chunk in chunks(reader, 100):
            chunk_list = list(chunk)
            ids = [x[0] for x in chunk_list]
            found_ids = [x.imdb_id for x in Movie.objects(imdb_id__in=ids).only('imdb_id')]
            data = [x for x in chunk_list if x[0] in found_ids]
            if data:
                import_imdb_ratings_task.delay(data)
        Log(type="import", message='import_imdb_ratings').save()
    else:
        log(layer=layer, message=f"Exception: {response.status_code} - {response.content}")


@monitor(monitor_slug='import_imdb_alt_titles')
def import_imdb_alt_titles():
    """titleId ordering title region language types attributes isOriginalTitle
    columns of interest: titleId, title, region
    """
    print("Dowloading title.akas.tsv.gz")
    url = 'https://datasets.imdbws.com/title.akas.tsv.gz'
    layer = get_channel_layer()
    log(layer=layer, message=f"Downloading file: {url}")
    response = requests.get(url, timeout=300)
    with open('title.akas.tsv.gz', 'wb') as f:
        f.write(response.content)
    if response.status_code == 200:
        contents = __unzip_file('title.akas.tsv.gz')
        csv.field_size_limit(sys.maxsize)

        reader = csv.reader(contents, delimiter='\t', quoting=csv.QUOTE_NONE)
        print("Processing IMDB Titles")

        next(reader)
        for chunk in chunks(reader, 100):
            chunk_list = list(chunk)
            ids = [x[0] for x in chunk_list]
            found_ids = [x.imdb_id for x in Movie.objects(imdb_id__in=ids).only('imdb_id')]
            data = [x for x in chunk_list if x[0] in found_ids]
            if data:
                import_imdb_titles_task.delay(data)
        Log(type="import", message='import_imdb_alt_titles').save()
        print("Done")
    else:
        log(layer=layer, message=f"Exception: {response.status_code} - {response.content}")
