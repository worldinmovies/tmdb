import decimal

from datetime import datetime, timedelta
import pytz
from mongoengine import DynamicDocument
from mongoengine.fields import (ListField,
                                EmbeddedDocumentField,
                                EmbeddedDocument,
                                ReferenceField,
                                StringField,
                                IntField,
                                BooleanField,
                                FloatField,
                                DateTimeField)
from babel.languages import get_official_languages

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
    titles = ListField((EmbeddedDocumentField(Title)))

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
    cast = ListField(EmbeddedDocumentField(Cast))
    crew = ListField(EmbeddedDocumentField(Crew))

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
        return f"iso:{self.id}, name:{self.name}"


class Genre(DynamicDocument):
    id = IntField(primary_key=True)
    name = StringField()

    def __str__(self):
        return f"id:{self.id}, name:{self.name}"


class SpokenLanguage(DynamicDocument):
    iso_639_1 = StringField(primary_key=True, max_length=4)
    name = StringField(max_length=50)

    def __str__(self):
        return f"iso:{self.iso_639_1}, name:{self.name}"


class FlattenedSpokenLanguage(EmbeddedDocument):
    iso = StringField()
    name = StringField()

    def __str__(self):
        return f"iso:{self.iso}, name:{self.name}"


class ProductionCountries(DynamicDocument):
    iso_3166_1 = StringField(primary_key=True, max_length=4)
    name = StringField(max_length=50)

    def __str__(self):
        return f"iso:{self.iso_3166_1}, name:{self.name}"


class FlattenedProductionCountry(EmbeddedDocument):
    iso = StringField()
    name = StringField()

    def __str__(self):
        return f"iso:{self.iso}, name:{self.name}"


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
    alternative_titles = EmbeddedDocumentField(AlternativeTitles)
    credits = EmbeddedDocumentField(Credits)
    external_ids = EmbeddedDocumentField(ExternalIDS)
    images = EmbeddedDocumentField(Images)

    meta = {'indexes': ['imdb_id']}

    def __str__(self):
        return (f"id:{self.id}, "
                f"genres:{self.genres}, "
                f"title:{self.title}")


class FlattenedMovie(DynamicDocument):
    id = IntField(primary_key=True)
    backdrop_path = StringField()
    belongs_to_collection = EmbeddedDocumentField(BelongsToCollection)
    budget = IntField()
    genres = ListField(StringField())
    homepage = StringField()
    imdb_id = StringField()
    original_language = StringField()
    original_title = StringField()
    overview = StringField()
    popularity = FloatField()
    poster_path = StringField()
    production_companies = ListField(EmbeddedDocumentField(ProductionCompany))
    production_countries = ListField(EmbeddedDocumentField(FlattenedProductionCountry))
    release_date = StringField()
    revenue = IntField()
    runtime = IntField()
    spoken_languages = ListField(EmbeddedDocumentField(FlattenedSpokenLanguage))
    status = StringField()
    tagline = StringField()
    title = StringField()
    vote_average = FloatField(default=0)
    imdb_vote_average = FloatField(default=0)
    vote_count = IntField(default=0)
    imdb_vote_count = IntField(default=0)
    alternative_titles = EmbeddedDocumentField(AlternativeTitles)
    credits = EmbeddedDocumentField(Credits)
    external_ids = EmbeddedDocumentField(ExternalIDS)
    images = EmbeddedDocumentField(Images)
    weighted_rating = FloatField()
    guessed_countries = ListField(StringField())

    meta = {'indexes': ['imdb_id', 'weighted_rating', 'guessed_country']}

    @staticmethod
    def create(movie: MovieDetails):
        return FlattenedMovie(id=movie['id'],
                              backdrop_path=movie['backdrop_path'],
                              belongs_to_collection=movie['belongs_to_collection'],
                              budget=movie['budget'],
                              genres=[x['name'] for x in movie['genres']],
                              homepage=movie['homepage'],
                              imdb_id=movie['imdb_id'],
                              original_language=movie['original_language'],
                              original_title=movie['original_title'],
                              overview=movie['overview'],
                              popularity=movie['popularity'],
                              poster_path=movie['poster_path'],
                              production_companies=movie['production_companies'],
                              production_countries=[{"iso": x['iso_3166_1'], "name": x['name']} for x in
                                                    movie['production_countries']],
                              release_date=movie['release_date'],
                              revenue=movie['revenue'],
                              runtime=movie['runtime'],
                              spoken_languages=[{"iso": x['iso_639_1'], "name": x['name']} for x in
                                                movie['spoken_languages']],
                              status=movie['status'],
                              tagline=movie['tagline'],
                              title=movie['title'],
                              vote_average=movie['vote_average'],
                              imdb_vote_average=0,
                              vote_count=movie['vote_count'],
                              imdb_vote_count=0,
                              alternative_titles=movie['alternative_titles'],
                              credits=movie['credits'],
                              external_ids=movie['external_ids'],
                              images=movie['images'],
                              weighted_rating=FlattenedMovie.calculate_weighted_rating_bayes(movie),
                              guessed_countries=FlattenedMovie.guess_countries(movie))

    @staticmethod
    def calculate_weighted_rating_bayes(movie: MovieDetails):
        """
        The formula for calculating the Top Rated 250 Titles gives a true Bayesian estimate:
        weighted rating (WR) = (v ÷ (v+m)) × R + (m ÷ (v+m)) × C where:

        R = average for the movie (mean) = (Rating)
        v = number of votes for the movie = (votes)
        m = minimum votes required to be listed in the Top 250 (currently 25000)
        C = the mean vote across the whole report (currently 7.0)
        """
        v = decimal.Decimal(movie['vote_count'])
        m = decimal.Decimal(200)
        r = decimal.Decimal(movie['vote_average'])
        c = decimal.Decimal(4)
        return (v / (v + m)) * r + (m / (v + m)) * c

    @staticmethod
    def guess_countries(movie: MovieDetails):
        orig_lang = movie['original_language']
        countries = [x['iso_3166_1'] for x in movie['production_countries'] if x]

        for country in [country for country in countries]:
            official_langs = get_official_languages(territory=country, de_facto=True, regional=True)
            if orig_lang in official_langs:
                return [country]
        if countries:
            return [countries[0]]
        else:
            return []


class Movie(DynamicDocument):
    id = IntField(primary_key=True)
    data = EmbeddedDocumentField(MovieDetails)
    fetched = BooleanField(required=True, default=False)
    fetched_date = DateTimeField()
    flattened_movie = ReferenceField(FlattenedMovie)

    @staticmethod
    def add_references(all_genres: dict[Genre],
                       all_langs: dict[SpokenLanguage],
                       all_countries: dict[ProductionCountries],
                       data: MovieDetails):
        data['production_countries'] = [all_countries[country['iso_3166_1']] for country in
                                        data['production_countries']]
        data['spoken_languages'] = [all_langs[lang['iso_639_1']] for lang in data['spoken_languages']]
        data['genres'] = [all_genres[genre['id']] for genre in data['genres']]
        return data

    def add_fetched_info(self, fetched_movie: MovieDetails):
        self.data = fetched_movie
        self.fetched = True
        self.fetched_date = datetime.now(tz)

    def __str__(self):
        return (f"id: {self.id}, "
                f"fetched: {self.fetched}, "
                f"data: {self.data}, "
                f"fetched_date: {self.fetched_date.isoformat() if self.fetched_date else None}"
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
