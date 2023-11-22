import decimal

import datetime
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

    def calculate_weighted_rating_bayes(self):
        """
        The formula for calculating the Top Rated 250 Titles gives a true Bayesian estimate:
        weighted rating (WR) = (v ÷ (v+m)) × R + (m ÷ (v+m)) × C where:

        R = average for the movie (mean) = (Rating)
        v = number of votes for the movie = (votes)
        m = minimum votes required to be listed in the Top 250 (currently 25000)
        C = the mean vote across the whole report (currently 7.0)
        """

        v = decimal.Decimal(self.vote_count) + \
            decimal.Decimal(self.imdb_vote_count)
        m = decimal.Decimal(200)
        r = decimal.Decimal(self.vote_average) + \
            decimal.Decimal(self.imdb_vote_average)
        c = decimal.Decimal(4)
        return (v / (v + m)) * r + (m / (v + m)) * c


class Movie(DynamicDocument):
    id = IntField(primary_key=True)
    data = EmbeddedDocumentField(MovieDetails)
    fetched = BooleanField(required=True, default=False)
    fetched_date = DateTimeField()
    flattened_movie = ReferenceField(FlattenedMovie)

    @staticmethod
    def add_references(all_genres, all_langs, all_countries, data):
        data['production_countries'] = [all_countries[country['iso_3166_1']] for country in
                                        data['production_countries']]
        data['spoken_languages'] = [all_langs[lang['iso_639_1']] for lang in data['spoken_languages']]
        data['genres'] = [all_genres[genre['id']] for genre in data['genres']]
        return data

    def add_fetched_info(self, fetched_movie: MovieDetails):
        self.data = fetched_movie
        self.fetched = True
        self.fetched_date = datetime.datetime.now(pytz.timezone('Europe/Stockholm'))

    def __str__(self):
        return (f"id: {self.id}, "
                f"fetched: {self.fetched}, "
                f"data: {self.data}, "
                f"fetched_date: {self.fetched_date.isoformat() if self.fetched_date else None}"
                )
