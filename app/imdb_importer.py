import csv
import difflib
import os
import requests
import sys

from sentry_sdk.crons import monitor
from channels.layers import get_channel_layer

from app.celery_tasks import import_imdb_ratings_task, import_imdb_titles_task
from app.helper import chunks, __unzip_file, log
from app.models import Log


def diff_files_and_exclude_same_lines(filename):
    new_contents = __unzip_file(filename)
    if os.path.exists(f"{filename}.old"):
        old_contents = __unzip_file(f"{filename}.old")
        included_count = 0
        # Process the difference and yield each line

        new_contents_list = list(new_contents)
        for line in difflib.ndiff(list(old_contents), new_contents_list):
            if line.startswith('+ '):
                included_count += 1
                yield line[2:]

        total_new_lines = len(new_contents_list)
        log(f"Excluded {(total_new_lines - included_count)} "
            f"lines that have already been imported from {total_new_lines}")
    else:
        yield from new_contents


def rename_file_to_old(filename):
    old = f"{filename}.old"
    try:
        if os.path.exists(old):
            os.remove(old)
        os.rename(filename, old)
    except FileNotFoundError:
        pass


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
    log(layer=layer, message=f"Downloading file: {url}")
    if response.status_code == 200:
        with open('title.ratings.tsv.gz', 'wb') as f:
            f.write(response.content)
        contents = diff_files_and_exclude_same_lines('title.ratings.tsv.gz')
        reader = csv.reader(contents, delimiter='\t')
        for chunk in chunks(reader, 100):
            import_imdb_ratings_task.delay(list(chunk))
        Log(type="import", message='import_imdb_ratings').save()
        rename_file_to_old('title.ratings.tsv.gz')
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
    response = requests.get(url)
    with open('title.akas.tsv.gz', 'wb') as f:
        f.write(response.content)
    if response.status_code == 200:
        contents = diff_files_and_exclude_same_lines('title.akas.tsv.gz')
        csv.field_size_limit(sys.maxsize)

        reader = csv.reader(contents, delimiter='\t', quoting=csv.QUOTE_NONE)
        print("Processing IMDB Titles")

        for chunk in chunks(reader, 100):
            import_imdb_titles_task.delay(list(chunk))
        Log(type="import", message='import_imdb_alt_titles').save()
        rename_file_to_old('title.akas.tsv.gz')
        print("Done")
    else:
        log(layer=layer, message=f"Exception: {response.status_code} - {response.content}")
