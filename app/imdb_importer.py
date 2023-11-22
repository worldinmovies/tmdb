import csv
import requests
import sys

from sentry_sdk.crons import monitor
from django.db import transaction
from channels.layers import get_channel_layer

from app.celery_tasks import import_imdb_ratings_task, import_imdb_titles_task
from app.helper import chunks, __send_data_to_channel, __unzip_file, __log_progress
from app.models import Movie, AlternativeTitles, FlattenedMovie


@monitor(monitor_slug='import_imdb_ratings')
def import_imdb_ratings():
    """Data-dump of imdbs ratings of all films
       TSV Headers are: tconst, averageRating, numVotes
       and file is about 1 million rows, which takes awhile to process...
       While we only have around 450k rows in our database.
    """
    url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    response = requests.get(url)
    layer = get_channel_layer()
    __send_data_to_channel(layer=layer, message=f"Downloading file: {url}")
    with open('title.ratings.tsv.gz', 'wb') as f:
        f.write(response.content)
    if response.status_code == 200:
        contents = __unzip_file('title.ratings.tsv.gz')
        length = len(contents)
        reader = csv.reader(contents, delimiter='\t')
        next(reader)
        for chunk in chunks(__log_progress(reader, "Processing IMDB Titles", length), 100):
            import_imdb_ratings_task.delay(list(chunk))
    else:
        __send_data_to_channel(layer=layer, message=f"Exception: {response.status_code} - {response.content}")


@monitor(monitor_slug='import_imdb_alt_titles')
def import_imdb_alt_titles():
    """titleId ordering title region language types attributes isOriginalTitle
    columns of interest: titleId, title, region
    """
    print("Dowloading title.akas.tsv.gz")
    url = 'https://datasets.imdbws.com/title.akas.tsv.gz'
    layer = get_channel_layer()
    __send_data_to_channel(layer=layer, message=f"Downloading file: {url}")
    response = requests.get(url)
    with open('title.akas.tsv.gz', 'wb') as f:
        f.write(response.content)
    if response.status_code == 200:
        contents = __unzip_file('title.akas.tsv.gz')
        count = len(contents)
        csv.field_size_limit(sys.maxsize)

        reader = csv.reader(contents, delimiter='\t', quoting=csv.QUOTE_NONE)
        print("Processing IMDB Titles")
        next(reader)  # Skip header

        for chunk in chunks(__log_progress(reader, "Processing IMDB Titles", count), 100):
            import_imdb_titles_task.delay(list(chunk))
        print("Done")
    else:
        __send_data_to_channel(layer=layer, message=f"Exception: {response.status_code} - {response.content}")
