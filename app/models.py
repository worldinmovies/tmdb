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
    cast_id = IntField()
    character = StringField()
    credit_id = StringField()
    gender = IntField()
    id = IntField()
    name = StringField()
    order = IntField()
    profile_path = StringField()

    def __str__(self):
        return f"id:{self.id}, name:{self.name}"


class Crew(EmbeddedDocument):
    credit_id = StringField()
    department = StringField()
    gender = IntField()
    id = IntField()
    job = StringField()
    name = StringField()
    profile_path = StringField()

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

    def __str__(self):
        return f"imdb_id:{self.imdb_id}, facebook_id:{self.facebook_id}"


class Images(EmbeddedDocument):
    backdrops = ListField(StringField())
    posters = ListField(StringField())

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


class ProductionCountries(DynamicDocument):
    iso_3166_1 = StringField(primary_key=True, max_length=4)
    name = StringField(max_length=50)

    def __str__(self):
        return f"iso:{self.iso_3166_1}, name:{self.name}"


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
    release_date = DateTimeField()
    revenue = IntField()
    runtime = IntField()
    spoken_languages = ListField(ReferenceField(SpokenLanguage, dbref=True, required=True))
    status = StringField()
    tagline = StringField()
    title = StringField()
    video = BooleanField()
    vote_average = FloatField()
    vote_count = IntField()
    alternative_titles = EmbeddedDocumentField(AlternativeTitles)
    credits = EmbeddedDocumentField(Credits)
    external_ids = EmbeddedDocumentField(ExternalIDS)
    images = EmbeddedDocumentField(Images)

    def __str__(self):
        return (f"id:{self.id}, "
                f"genres:{self.genres}"
                f"title:{self.title}")


class Movie(DynamicDocument):
    id = IntField(primary_key=True)
    data = EmbeddedDocumentField(MovieDetails)
    fetched = BooleanField(required=True, default=False)
    fetched_date = DateTimeField()

    def add_fetched_info(self, fetched_movie: MovieDetails):
        self.data = fetched_movie
        self.fetched = True
        self.fetched_date = datetime.datetime.now(pytz.timezone('Europe/Stockholm'))

    def __str__(self):
        return (f"id: {self.id}, "
                f"fetched: {self.fetched}, "
                f"data: {self.data}"
                f"fetched_at: {self.fetched_date.isoformat() if self.fetched_date else None}")
