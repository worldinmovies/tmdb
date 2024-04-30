import datetime
import json
import os
import time
import requests_mock
import codecs

from app.helper import get_statics
from app.models import SpokenLanguage, ProductionCountries, Genre, Movie
from behave import *


@given("all basics are present in mongo")
def all_basics(context):
    with open("testdata/genres.json", 'rb') as genres:
        [Genre(**x).save() for x in json.loads(genres.read()).get('genres')]
    with open("testdata/languages.json", 'rb') as langs:
        [SpokenLanguage(**x).save() for x in json.loads(langs.read())]
    with open("testdata/countries.json", 'rb') as countries:
        [ProductionCountries(**x).save() for x in json.loads(countries.read())]


@given(u'basics are present in mongo')
def given_basics_are_present(context):
    os.environ['TMDB_API'] = 'test'
    SpokenLanguage(iso_639_1='en', name='English').save()
    SpokenLanguage(iso_639_1='es', name='Spanish').save()
    SpokenLanguage(iso_639_1='sv', name='Swedish').save()
    SpokenLanguage(iso_639_1='ru', name='Russian').save()
    SpokenLanguage(iso_639_1='da', name='Danish').save()
    SpokenLanguage(iso_639_1='fi', name='Finnish').save()
    SpokenLanguage(iso_639_1='pl', name='Polish').save()
    SpokenLanguage(iso_639_1='fr', name='French').save()
    SpokenLanguage(iso_639_1='de', name='German').save()
    ProductionCountries(iso_3166_1='FI', name='Finland').save()
    ProductionCountries(iso_3166_1='US', name='United States of america').save()
    ProductionCountries(iso_3166_1='DK', name='Denmark').save()
    ProductionCountries(iso_3166_1='AU', name='Australia').save()
    ProductionCountries(iso_3166_1='GB', name='Great Britain').save()
    ProductionCountries(iso_3166_1='SE', name='Sweden').save()
    ProductionCountries(iso_3166_1='SU', name='Soviet').save()
    Genre(id=28, name="Action").save()
    Genre(id=36, name="History").save()
    Genre(id=12, name="Adventure").save()
    Genre(id=14, name="Fantasy").save()
    Genre(id=878, name="Science Fiction").save()
    Genre(id=18, name="Drama").save()
    Genre(id=10751, name="Family").save()


@given(u'movies "{json_data}" is persisted')
def given_movies_are_saved(context, json_data):
    for i in json.loads(json_data):
        fetched_date_str = i.get('fetched_date', None)
        fetched_date = datetime.date.fromisoformat(fetched_date_str) if fetched_date_str else None
        movie = Movie(id=i['id'],
                      fetched=i.get('fetched', False),
                      fetched_date=fetched_date)
        movie.add_fetched_info(dict(i),
                               Genre.objects.all(),
                               SpokenLanguage.objects.all(),
                               ProductionCountries.objects.all())
        movie.id = i['id']
        movie.fetched = i.get('fetched', False)
        movie.fetched_date = fetched_date
        movie.save()


@given("movies from file:{file} is persisted")
def persist_movie_from_file(context, file):
    with open(f"testdata/{file}", 'rb') as img1:
        data = json.loads(img1.read())
        all_genres, all_langs, all_countries = get_statics()

        movie = Movie(id=data['id'],
                      fetched=True,
                      fetched_date=None)
        movie.add_fetched_info(dict(data), all_genres, all_langs, all_countries)
        movie.save()


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
    print(f"Response was: {context.response.content.decode('utf-8')}")
    context.test.assertContains(response=context.response, text=expected)


@given("tmdb file is mocked with {data}")
def mock_tmdb_file(context, data):
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    yesterday_formatted = yesterday.strftime("%m_%d_%Y")
    daily_export_url = f"http://files.tmdb.org/p/exports/movie_ids_{yesterday_formatted}.json.gz"
    start_mock(context)
    with open(f"testdata/{data}", 'rb') as asd:
        context.mocker.get(daily_export_url, status_code=200, content=asd.read())


@given("tmdb data is mocked with {data} for id {movie_id} with status {status}")
def mock_tmdb_data(context, data, movie_id, status):
    url = "https://api.themoviedb.org/3/movie/{movie_id}?" \
          "api_key={api_key}&" \
          "language=en-US&" \
          "append_to_response=alternative_titles,credits,external_ids,images,account_states," \
          "recommendations,watch/providers".format(
        api_key='test', movie_id=movie_id)
    start_mock(context)
    with open(f"testdata/{data}", 'rb') as asd:
        context.mocker.get(url, status_code=int(status), content=asd.read())


@then("after awhile there should be {amount} movies persisted")
def wait_for_persistence(context, amount):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects, int(amount)),
        f"Movies in database: {Movie.objects.all()}, expected {amount}")


def wait_function_is_true(clazz, amount, timeout: float = 5, period=0.1):
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
                              .filter(imdb_id=imdb_id, imdb_vote_average__gt=0),
                              1), f"Movie with imdb_id={imdb_id} "
                                  f"should be found: {Movie.objects.filter(imdb_id=imdb_id).all()}")
    movie: Movie = Movie.objects.get(imdb_id=imdb_id)
    context.test.assertTrue(movie.imdb_vote_average > 0, f"Value should have been more than 0, but was: "
                                                         f"{movie.imdb_vote_average}")
    context.test.assertTrue(movie.imdb_vote_count > 0, f"Value should have been more than 0, but was: "
                                                       f"{movie.imdb_vote_count}")


@then("imdb_id={imdb_id} should have imdb_alt_titles {expected_titles} set eventually")
def expect_alt_titles_be_set(context, imdb_id, expected_titles):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects
                              .filter(imdb_id=imdb_id, alternative_titles__titles__1__exists=True)
                              , 1), f"Movie with imdb_id={imdb_id} "
                                    f"should be found: {Movie.objects.filter(imdb_id=imdb_id).all()}")
    movie: Movie = Movie.objects.get(imdb_id=imdb_id)
    actual_titles = [x['title'] for x in movie.alternative_titles.titles]
    context.test.assertEqual(set(expected_titles.split(',')), set(actual_titles))


@then("id={movie_id} should have alt_titles set eventually")
def expect_alt_titles_be_set(context, movie_id):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects
                              .filter(id=movie_id, alternative_titles__titles__1__exists=True)
                              , 1), f"Movie with id={movie_id} "
                                    f"should be found: {Movie.objects.filter(id=movie_id).all()}")
    movie: Movie = Movie.objects.get(id=movie_id)
    actual_titles = [x['title'] for x in movie.alternative_titles.titles]
    context.test.assertTrue(actual_titles)


@then("id={movie_id} should have country set to {guessed_country} eventually")
def expect_guessed_country(context, movie_id, guessed_country):
    context.test.assertTrue(
        wait_function_is_true(Movie.objects
                              .filter(pk=movie_id, guessed_country__exact=guessed_country)
                              , 1, 2), f"Movie with id={movie_id} "
                                         f"should be found: {Movie.objects.filter(id=movie_id)
                                                             .only('id',
                                                                    'original_language',
                                                                    'guessed_country',
                                                                    'production_countries')}")


@then('response be "{expected}"')
def response_should_be(context, expected):
    with codecs.open(f"testdata/expected/{expected}", 'rb', 'utf-8') as file:
        expected_data = file.read()
        print(f"RESPONSE: {context.response.content.decode('unicode_escape')}")
        context.test.assertEqual(json.loads(context.response.content)[0], json.loads(expected_data)[0])

@step("guessed_country field is nulled for id={movie_id}")
def null_guessed_country(context, movie_id):
    movie = Movie.objects.get(id=movie_id)
    movie.guessed_country = None
    movie.save()
