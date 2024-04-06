import datetime
import json
import os
import time
import requests_mock

from app.models import SpokenLanguage, ProductionCountries, Genre, Movie, FlattenedMovie, MovieDetails
from behave import *


@given(u'basics are present in mongo')
def given_basics_are_present(context):
    os.environ['TMDB_API'] = 'test'
    SpokenLanguage(iso_639_1='en', name='English').save()
    SpokenLanguage(iso_639_1='es', name='Spanish').save()
    SpokenLanguage(iso_639_1='sv', name='Swedish').save()
    SpokenLanguage(iso_639_1='ru', name='Russian').save()
    ProductionCountries(iso_3166_1='US', name='United States of america').save()
    ProductionCountries(iso_3166_1='AU', name='Australia').save()
    ProductionCountries(iso_3166_1='GB', name='Great Britain').save()
    ProductionCountries(iso_3166_1='SE', name='Sweden').save()
    ProductionCountries(iso_3166_1='SU', name='Soviet').save()
    Genre(id=28, name="Action").save()
    Genre(id=12, name="Adventure").save()
    Genre(id=14, name="Fantasy").save()
    Genre(id=878, name="Science Fiction").save()
    Genre(id=18, name="Drama").save()


@given(u'movies "{json_data}" is persisted')
def given_movies_are_saved(context, json_data):
    for i in json.loads(json_data):
        fetched_date_str = i.get('fetched_date', None)
        fetched_date = datetime.date.fromisoformat(fetched_date_str) if fetched_date_str else None
        data = i.get('data', {})
        movie_details = Movie.add_references(
            Genre.objects.all(),
            SpokenLanguage.objects.all(),
            ProductionCountries.objects.all(),
            data)
        movie = Movie(id=i['id'],
                      fetched=i.get('fetched', False),
                      fetched_date=fetched_date)
        movie.add_fetched_info(MovieDetails(id=i['id'], **movie_details))
        movie.fetched = i.get('fetched', False)
        movie.fetched_date = fetched_date
        movie.save()


@given("movies from file:{file} is persisted")
def persist_movie_from_file(context, file):
    with open(f"testdata/{file}", 'rb') as img1:
        data = json.loads(img1.read())
        all_genres = dict([(gen.id, gen) for gen in Genre.objects.all()])
        all_langs = dict([(lang.iso_639_1, lang) for lang in SpokenLanguage.objects.all()])
        all_countries = dict([(country.iso_3166_1, country) for country in ProductionCountries.objects.all()])

        details = Movie.add_references(all_genres, all_langs, all_countries, data)
        FlattenedMovie.create(details).save()


@when(u'calling {url}')
def calling_url(context, url):
    context.response = context.test.client.get(url)


@then(u'http status should be {http_status}')
def verify_http_status(context, http_status):
    context.test.assertEqual(str(context.response.status_code), http_status)


@then(u'response should be {expected_response}')
def verify_content(context, expected_response):
    context.test.assertEqual(context.response.content.decode('utf-8'), str(expected_response), Movie.objects.all())


@then('response should contain "{expected}"')
def response_should_contain(context, expected):
    context.test.assertContains(context.response, expected)


@given("tmdb file is mocked with {data}")
def mock_tmdb_file(context, data):
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    yesterday_formatted = yesterday.strftime("%m_%d_%Y")
    daily_export_url = f"http://files.tmdb.org/p/exports/movie_ids_{yesterday_formatted}.json.gz"
    start_mock(context)
    with open(f"testdata/{data}", 'rb') as asd:
        context.mocker.get(daily_export_url, status_code=200, content=asd.read())


@given("tmdb data is mocked with {data} for id {id} with status {status}")
def mock_tmdb_data(context, data, id, status):
    url = "https://api.themoviedb.org/3/movie/{movie_id}?" \
          "api_key={api_key}&" \
          "language=en-US&" \
          "append_to_response=alternative_titles,credits,external_ids,images,account_states".format(
        api_key='test', movie_id=id)
    start_mock(context)
    with open(f"testdata/{data}", 'rb') as asd:
        context.mocker.get(url, status_code=int(status), content=asd.read())


@then("after awhile there should be {amount} movies persisted")
def wait_for_persistence(context, amount):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects, int(amount)), f"Movies in database: {Movie.objects.all()}, expected {amount}")


def wait_function_is_true(clazz, amount, timeout=5, period=0.5):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if clazz.all().count() == amount:
            return True
        time.sleep(period)
    return False


@given("base data {path} is mocked with {mocked_data}")
def base_data(context, path, mocked_data):
    url = f"https://api.themoviedb.org/3{path}"
    start_mock(context)
    with open(f"testdata/{mocked_data}", 'rb') as asd:
        context.mocker.get(url, status_code=200, content=asd.read())


def start_mock(context):
    if not hasattr(context, 'mocker'):
        context.mocker = requests_mock.Mocker()
        context.mocker.start()
        context.mocker.get(requests_mock.ANY, status_code=404, text="Request was not matched")


@then("there should be {countries} countries persisted")
def countries_persisted(context, countries):
    wait_function_is_true(ProductionCountries.objects, int(countries))
    context.test.assertEqual(ProductionCountries.objects.count(), int(countries))


@then("there should be {languages} languages persisted")
def langs_persisted(context, languages):
    wait_function_is_true(SpokenLanguage.objects, int(languages))
    context.test.assertEqual(SpokenLanguage.objects.count(), int(languages))


@then("there should be {genres} genres persisted")
def genres_persisted(context, genres):
    wait_function_is_true(Genre.objects, int(genres))
    context.test.assertEqual(Genre.objects.count(), int(genres))


@given("basics are removed from mongo")
def clean_before(context):
    ProductionCountries.objects.all().delete()
    SpokenLanguage.objects.all().delete()
    Genre.objects.all().delete()


@then('{movie_id} should have "fetched" set to "False"')
def verify_not_fetched(context, movie_id):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects
                              .filter(pk=int(movie_id))
                              .filter(fetched=False), 1))


@given('{url} is mocked with {file}')
def mock_url_with_file(context, url, file):
    start_mock(context)
    with open(f"testdata/{file}", 'rb') as asd:
        context.mocker.get(url, status_code=200, content=asd.read())


@then("imdb_id={imdb_id} should have imdb_ratings set eventually")
def expect_imdb_ratings_be_set(context, imdb_id):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects
                              .filter(data__imdb_id=imdb_id)
                              .filter(data__imdb_vote_average__gt=0), 1), f"Movie with imdb_id={imdb_id} "
                                                                          f"should be found")
    movie = Movie.objects.get(data__imdb_id=imdb_id)
    context.test.assertTrue(movie.data.imdb_vote_average > 0, f"Value should have been more than 0, but was: "
                                                              f"{movie.data.imdb_vote_average}")
    context.test.assertTrue(movie.data.imdb_vote_count > 0, f"Value should have been more than 0, but was: "
                                                            f"{movie.data.imdb_vote_count}")


@then("imdb_id={imdb_id} should have imdb_alt_titles {expected_titles} set eventually")
def expect_alt_titles_be_set(context, imdb_id, expected_titles):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects
                              .filter(data__imdb_id=imdb_id)
                              .filter(data__alternative_titles__titles__1__exists=True)
                              , 1), f"Movie with imdb_id={imdb_id} "
                                    f"should be found")
    movie = Movie.objects.get(data__imdb_id=imdb_id)
    actual_titles = [x['title'] for x in movie.data.alternative_titles.titles]
    context.test.assertEqual(set(expected_titles.split(',')), set(actual_titles))
