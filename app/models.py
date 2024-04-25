import decimal
from collections import Counter
from typing import override
from bson import json_util
import pycountry
import pytz

from datetime import datetime, timedelta

from mongoengine import DynamicDocument, QuerySet
from mongoengine.fields import (ListField,
                                EmbeddedDocumentField,
                                EmbeddedDocument,
                                ReferenceField,
                                StringField,
                                IntField,
                                BooleanField,
                                FloatField,
                                DateTimeField, EmbeddedDocumentListField)
from babel.languages import get_official_languages, get_territory_language_info

tz = pytz.timezone('Europe/Stockholm')


class Title(EmbeddedDocument):
    iso_3166_1 = StringField(max_length=8)
    title = StringField()
    type = StringField()

    def __init__(self, iso_3166_1, title, type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if len(iso_3166_1) > 8:
            self.iso_3166_1 = 'XX'
        else:
            self.iso_3166_1 = iso_3166_1
        self.title = title
        self.type = type

    def __str__(self):
        return f"iso:{self.iso_3166_1}, title:{self.title}"


class AlternativeTitles(EmbeddedDocument):
    titles = EmbeddedDocumentListField(Title)

    def __str__(self):
        return f"titles:{self.titles}"


class BelongsToCollection(EmbeddedDocument):
    id = IntField()
    name = StringField()
    poster_path = StringField()
    backdrop_path = StringField()

    def __str__(self):
        return f"id:{self.id}, name:{self.name}"


class Cast(EmbeddedDocument):
    adult = BooleanField()
    cast_id = IntField()
    character = StringField()
    credit_id = StringField()
    gender = IntField()
    id = IntField()
    name = StringField()
    order = IntField()
    profile_path = StringField()
    popularity = FloatField()
    original_name = StringField()
    known_for_department = StringField()

    def __str__(self):
        return f"id:{self.id}, name:{self.name}"


class Crew(EmbeddedDocument):
    adult = BooleanField()
    credit_id = StringField()
    department = StringField()
    gender = IntField()
    id = IntField()
    job = StringField()
    name = StringField()
    profile_path = StringField()
    popularity = FloatField()
    original_name = StringField()
    known_for_department = StringField()

    def __str__(self):
        return f"id:{self.id}, name:{self.name}"


class Credits(EmbeddedDocument):
    cast = EmbeddedDocumentListField(Cast)
    crew = EmbeddedDocumentListField(Crew)

    def __str__(self):
        return f"cast:{self.cast}, crew:{self.crew}"


class ExternalIDS(EmbeddedDocument):
    imdb_id = StringField()
    facebook_id = StringField()
    instagram_id = StringField()
    twitter_id = StringField()
    wikidata_id = StringField()

    def __str__(self):
        return f"imdb_id:{self.imdb_id}, facebook_id:{self.facebook_id}"


class Images(EmbeddedDocument):
    backdrops = ListField(StringField())
    posters = ListField(StringField())
    logos = ListField(StringField())

    def __str__(self):
        return f"iso:{self.backdrops}, name:{self.posters}"


class ProductionCompany(EmbeddedDocument):
    id = IntField()
    logo_path = StringField()
    name = StringField()
    origin_country = StringField()

    def __str__(self):
        return f"{{id:\"{self.id}\", name:\"{self.name}\", country:\"{self.origin_country}\"}}"


class Genre(DynamicDocument):
    id = IntField(primary_key=True)
    name = StringField()

    def __str__(self):
        return f"{{id:\"{self.id}\", name:\"{self.name}\"}}"


class SpokenLanguage(DynamicDocument):
    iso_639_1 = StringField(primary_key=True, max_length=4)
    name = StringField(max_length=50)

    def __str__(self):
        return f"{{iso:\"{self.iso_639_1}\", name:\"{self.name}\"}}"


class ProductionCountries(DynamicDocument):
    iso_3166_1 = StringField(primary_key=True, max_length=4)
    name = StringField(max_length=50)

    def __str__(self):
        return f"{{iso:\"{self.iso_3166_1}\", name:\"{self.name}\"}}"


class MovieDetails(EmbeddedDocument):
    adult = BooleanField()
    backdrop_path = StringField()
    belongs_to_collection = EmbeddedDocumentField(BelongsToCollection)
    budget = IntField()
    genres = ListField(ReferenceField(Genre, dbref=True, required=True))
    homepage = StringField()
    id = IntField()
    imdb_id = StringField()
    original_language = StringField()
    original_title = StringField()
    overview = StringField()
    popularity = FloatField()
    poster_path = StringField()
    production_companies = ListField(EmbeddedDocumentField(ProductionCompany))
    production_countries = ListField(ReferenceField(ProductionCountries, dbref=True, required=True))
    release_date = StringField()
    revenue = IntField()
    runtime = IntField()
    spoken_languages = ListField(ReferenceField(SpokenLanguage, dbref=True, required=True))
    status = StringField()
    tagline = StringField()
    title = StringField()
    video = BooleanField()
    vote_average = FloatField(default=0)
    imdb_vote_average = FloatField(default=0)
    vote_count = IntField(default=0)
    imdb_vote_count = IntField(default=0)
    weighted_rating = FloatField()
    alternative_titles = EmbeddedDocumentField(AlternativeTitles)
    credits = EmbeddedDocumentField(Credits)
    external_ids = EmbeddedDocumentField(ExternalIDS)
    images = EmbeddedDocumentField(Images)

    meta = {'indexes': ['imdb_id', 'weighted_rating']}

    def __str__(self):
        return (f"{{id:'{self.id}', "
                f"imdb_id:'{self.imdb_id}', "
                f"genres:'{self.genres}', "
                f"weighted_rating:'{self.weighted_rating}', "
                f"title:'{self.title}'}}")


class CustomQuerySet(QuerySet):
    def to_json(self):
        return "[%s]" % (",".join([doc.to_json() for doc in self]))


class Movie(DynamicDocument):
    id = IntField(primary_key=True)
    fetched = BooleanField(required=True, default=False)
    fetched_date = DateTimeField()
    # Deprecated
    data = EmbeddedDocumentField(MovieDetails)

    backdrop_path = StringField()
    belongs_to_collection = EmbeddedDocumentField(BelongsToCollection)
    budget = IntField()
    genres = ListField(ReferenceField(Genre, dbref=True, required=True))
    homepage = StringField()
    imdb_id = StringField()
    original_language = StringField()
    origin_country = ListField(StringField())
    original_title = StringField()
    overview = StringField()
    popularity = FloatField()
    poster_path = StringField()
    production_companies = EmbeddedDocumentListField(ProductionCompany)
    production_countries = ListField(ReferenceField(ProductionCountries, dbref=True, required=True))
    release_date = StringField()
    revenue = IntField()
    runtime = IntField()
    spoken_languages = ListField(ReferenceField(SpokenLanguage, dbref=True, required=True))
    status = StringField()
    tagline = StringField()
    title = StringField()
    video = BooleanField()
    vote_average = FloatField(default=0)
    imdb_vote_average = FloatField(default=0)
    vote_count = IntField(default=0)
    imdb_vote_count = IntField(default=0)
    weighted_rating = FloatField(default=0)
    alternative_titles = EmbeddedDocumentField(AlternativeTitles)
    credits = EmbeddedDocumentField(Credits)
    external_ids = EmbeddedDocumentField(ExternalIDS)
    images = EmbeddedDocumentField(Images)
    guessed_country = StringField()

    meta = {'indexes': ['imdb_id', 'weighted_rating', 'guessed_country'],
            'queryset_class': CustomQuerySet}

    def add_fetched_info(self, movie: dict, all_genres: dict[Genre],
                         all_langs: dict[SpokenLanguage],
                         all_countries: dict[ProductionCountries]):
        self.fetched = True
        self.fetched_date = datetime.now(tz)
        self.add_references(movie, all_genres, all_langs, all_countries)
        self.backdrop_path = movie.get('backdrop_path')
        self.belongs_to_collection = BelongsToCollection(**movie.get('belongs_to_collection')) if movie.get(
            'belongs_to_collection') else None
        self.budget = movie.get('budget')
        self.homepage = movie.get('homepage')
        self.imdb_id = movie.get('imdb_id')
        self.original_language = movie.get('original_language')
        self.original_title = movie.get('original_title')
        self.overview = movie.get('overview')
        self.popularity = movie.get('popularity')
        self.poster_path = movie.get('poster_path')
        self.production_companies = [ProductionCompany(**x) for x in movie.get('production_companies', [])]
        self.release_date = movie.get('release_date')
        self.origin_country = movie.get('origin_country')
        self.revenue = movie.get('revenue')
        self.runtime = movie.get('runtime')
        self.status = movie.get('status')
        self.tagline = movie.get('tagline')
        self.title = movie.get('title')
        self.vote_average = movie.get('vote_average')
        self.vote_count = movie.get('vote_count', 0)
        self.alternative_titles = AlternativeTitles(**movie.get('alternative_titles')) if movie.get(
            'alternative_titles') else None
        self.credits = Credits(**movie.get('credits')) if movie.get('credits') else None
        self.external_ids = ExternalIDS(**movie.get('external_ids')) if movie.get('external_ids') else None
        self.images = Images(**movie.get('images')) if movie.get('images') else None
        self.calculate_weighted_rating_bayes()
        self.guess_country()

    def calculate_weighted_rating_bayes(self):
        """
        The formula for calculating the Top Rated 250 Titles gives a true Bayesian estimate:
        weighted rating (WR) = (v ÷ (v+m)) × R + (m ÷ (v+m)) × C where:

        R = average for the movie (mean) = (Rating)
        v = number of votes for the movie = (votes)
        m = minimum votes required to be listed in the Top 250 (currently 25000)
        C = the mean vote across the whole report (currently 7.0)
        """
        v = decimal.Decimal(self.vote_count) + decimal.Decimal(self.imdb_vote_count)
        m = decimal.Decimal(200)
        if self.imdb_vote_count > 0:
            r = (decimal.Decimal(self.vote_average) + decimal.Decimal(self.imdb_vote_average)) / 2
        else:
            r = decimal.Decimal(self.vote_average)
        c = decimal.Decimal(4)
        self.weighted_rating = float((v / (v + m)) * r + (m / (v + m)) * c)

    def guess_country(self):
        def estimate_country_of_origin(origin_country_db, original_language, production_countries,
                                       production_companies):
            territories = []
            country: pycountry.db.Country
            for country in pycountry.countries:
                country_languages = get_official_languages(territory=country.alpha_2)
                if original_language in country_languages:
                    territories.append(country.alpha_2)

            territories_with_percentage = []
            # Get all countries that has this language as an official language
            for territory in territories:
                infos = get_territory_language_info(territory)
                for info in infos.items():
                    official_status = info[1].get('official_status')
                    percentage = info[1].get('population_percent')
                    if info[0] == original_language and official_status == 'official':
                        territories_with_percentage.append({"territory": territory, "percentage": percentage})

            territories_with_percentage.sort(key=lambda item: item.get('percentage'))

            # 0. If there's only one origin_country attribute set
            if origin_country_db and len(origin_country_db) == 1:
                origin_country = origin_country_db[0]
            # 1. If there's only one country related to the language
            elif len(territories_with_percentage) == 1:
                origin_country = territories_with_percentage[0].get('territory')
            # 2. If there's only one country among the production_countries
            elif len(production_countries) == 1:
                origin_country = production_countries[0]
            else:
                # If original language is spoken in multiple countries, consider all of them
                # Filter out countries where the language is spoken by less than 10% of the population

                # Count occurrences of production countries within the filtered list of origin countries
                territories_connected_to_production = [country for country in
                                                       territories_with_percentage if country.get('territory')
                                                       in production_companies]
                production_counter = Counter([x.get('territory') for x in territories_connected_to_production])
                commons = dict()
                [commons.setdefault(x[1], []).append(x[0]) for x in production_counter.items()]
                sorted(commons.items(), key=lambda x: x[0])
                most_common = list(commons.items())[-1] if len(commons.items()) > 0 else commons.items()

                # 3. There's a majority of production_countries, connected to the language
                if len(most_common[1]) == 1:
                    origin_country = most_common[1][0]
                # 4. Highest ranked territory based on population speakers of this language
                else:
                    sorted(territories_connected_to_production, key=lambda x: x.get('percentage'))
                    highest_ranked_country_on_lang = territories_connected_to_production[-1].get('territory')
                    # Pick the production country with the highest count
                    origin_country = highest_ranked_country_on_lang

            return origin_country

        orig_lang = self.original_language
        if orig_lang:
            self.guessed_country = estimate_country_of_origin(self.origin_country,
                                                              orig_lang,
                                                              [x['iso_3166_1'] for x in self.production_countries if x],
                                                              [x['origin_country'] for x in self.production_companies])

    def add_references(self,
                       data: dict,
                       all_genres: dict[Genre],
                       all_langs: dict[SpokenLanguage],
                       all_countries: dict[ProductionCountries]):
        self.production_countries = [all_countries[country['iso_3166_1']] for country
                                     in data.get('production_countries', [])]
        self.spoken_languages = [all_langs[lang['iso_639_1']] for lang in data.get('spoken_languages', [])]
        self.genres = [all_genres[genre['id']] for genre in data.get('genres', [])]

    @override
    def to_json(self):
        data = self.to_mongo()
        for i, genre in enumerate(data['genres']):
            data['genres'][i] = self.genres[i].to_mongo()
        for i, country in enumerate(data['production_countries']):
            x: dict = self.production_countries[i]
            data['production_countries'][i] = {
                "iso": x['iso_3166_1'],
                "name": x['english_name'] if x['english_name'] else x['name']}
        for i, langs in enumerate(data['spoken_languages']):
            x: dict = self.spoken_languages[i]
            data['spoken_languages'][i] = {
                "iso": x['iso_639_1'],
                "name": x['english_name'] if x['english_name'] else x['name']
            }
        return json_util.dumps(data)

    @override
    def __str__(self):
        return (f"{{id: '{self.id}', "
                f"fetched: '{self.fetched}', "
                f"fetched_date: '{self.fetched_date.isoformat() if self.fetched_date else None}', "
                f"imdb_id:'{self.imdb_id}', "
                f"data:'{self.data}', "
                f"genres:'{self.genres}', "
                f"weighted_rating:'{self.weighted_rating}', "
                f"guessed_country:'{self.guessed_country}', "
                f"alternative_titles:'{self.alternative_titles}', "
                f"original_title:'{self.original_title}', "
                f"title:'{self.title}'}}"
                )


class Log(DynamicDocument):
    meta = {'collection': 'log',
            'indexes': [
                {
                    'name': 'TTL_index',
                    'fields': ['ttl'],
                    'expireAfterSeconds': 0
                }
            ]}

    type = StringField(required=True)
    message = StringField()
    timestamp = DateTimeField(required=True, default=datetime.now(tz).now)
    ttl = DateTimeField(required=True, default=(datetime.now(tz) + timedelta(days=7)).now)

    @staticmethod
    def get_now_plus(days):
        return (datetime.now(tz) + timedelta(days=days)).now

    def __str__(self):
        return f"id:{self.pk}, type:{self.type}, message:{self.message}, timestamp: {self.timestamp}, ttl: {self.ttl}"
