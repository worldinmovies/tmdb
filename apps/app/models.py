from apps.app.db_models import Movie


class BelongsToCollection:
    def __init__(self, 
        id,
        name,
        poster_path,
        backdrop_path):
        self.id = id
        self.name = name
        self.poster_path = poster_path
        self.backdrop_path = backdrop_path

class ProductionCompany:
    def __init__(self, 
        id,
        logo_path,
        name,
        origin_country):
        self.id = id
        self.logo_path = logo_path
        self.name = name
        self.origin_country = origin_country

class ProductionCountries:
    def __init__(self, 
        iso, 
        name):
        self.iso = iso
        self.name = name

class Crew:
    def __init__(self, id):
        self.id = id

class Cast:
    def __init__(self, id):
        self.id = id


class Credit:
    def __init__(self, 
        crew: list[Crew], 
        cast: list[Cast]):
        self.crew = crew
        self.cast = cast


class ExternalIDs:
    def __init__(self, 
        id,
        imdb_id,
        facebook_id,
        instagram_id,
        twitter_id,
        wikidata_id):
        self.id = id
        self.imdb_id = imdb_id
        self.facebook_id = facebook_id
        self.instagram_id = instagram_id
        self.twitter_id = twitter_id
        self.wikidata_id = wikidata_id


class Provider:
    def __init__(self, id):
        self.id = id


class DetailedMovie:
    def __init__(self, 
        id,
        fetched: bool,
        fetched_date,
        backdrop_path,
        belongs_to_collection: BelongsToCollection,
        budget,
        genres: list[str],
        homepage,
        imdb_id,
        original_language,
        origin_country,
        original_title,
        overview,
        popularity,
        poster_path,
        production_companies: list[ProductionCompany],
        production_countries: list[ProductionCountries],
        release_date,
        revenue,
        runtime,
        spoken_languages: list[str],
        status,
        tagline,
        title,
        video,
        vote_average,
        vote_count,
        imdb_vote_average,
        imdb_vote_count,
        weighted_rating,
        alternative_titles: list[str],
        credits: list[Credit],
        external_ids: ExternalIDs,
        images: list[str],
        recommended_movies: list[str],
        providers: list[Provider],
        guessed_country):
        self.id = id
        self.fetched = fetched
        self.fetched_date = fetched_date
        self.backdrop_path = backdrop_path
        self.belongs_to_collection = belongs_to_collection
        self.budget = budget
        self.genres = genres
        self.homepage = homepage
        self.imdb_id = imdb_id
        self.original_language = original_language
        self.origin_country = origin_country
        self.original_title = original_title
        self.overview = overview
        self.popularity = popularity
        self.poster_path = poster_path
        self.production_companies = production_companies
        self.production_countries = production_countries
        self.release_date = release_date
        self.revenue = revenue
        self.runtime = runtime
        self.spoken_languages = spoken_languages
        self.status = status
        self.tagline = tagline
        self.title = title
        self.video = video
        self.vote_average = vote_average
        self.vote_count = vote_count
        self.imdb_vote_average = imdb_vote_average
        self.imdb_vote_count = imdb_vote_count
        self.weighted_rating = weighted_rating
        self.alternative_titles = alternative_titles
        self.credits = credits
        self.external_ids = external_ids
        self.images = images
        self.recommended_movies = recommended_movies
        self.providers = providers
        self.guessed_country = guessed_country



class SearchMovie:
    def __init__(self, 
        id, 
        title,
        original_title,
        overview,
        directors,
        weighted_rating,
        vote_average,
        vote_count,
        guessed_country,
        original_language,
        poster,
        year):
        self.id = id
        self.title = title
        self.original_title = original_title
        self.overview = overview
        self.directors = directors
        self.weighted_rating = weighted_rating
        self.vote_average = vote_average
        self.vote_count = vote_count
        self.guessed_country = guessed_country
        self.original_language = original_language
        self.poster = poster
        self.year = year


class DiscoveryMovie:
    def __init__(self, 
        id, 
        imdb_id,
        original_title, 
        english_title, 
        poster_path, 
        vote_average, 
        vote_count, 
        estimated_country, 
        year, 
        director,
        genres, 
        weighted_rating, 
        overview):
        self.id = id
        self.imdb_id = imdb_id
        self.original_title = original_title
        self.english_title = english_title
        self.poster_path = poster_path
        self.vote_average = vote_average
        self.vote_count = vote_count
        self.estimated_country = estimated_country
        self.year = year
        self.director = director
        self.genres = genres
        self.weighted_rating = weighted_rating
        self.overview = overview


class ImdbImportMovie:
    def __init__(self, 
        id, 
        imdb_id,
        original_title,
        release_date,
        poster_path,
        vote_average,
        vote_count,
        country_code):
        self.id = id
        self.imdb_id = imdb_id
        self.original_title = original_title
        self.release_date = release_date
        self.poster_path = poster_path
        self.vote_average = vote_average
        self.vote_count = vote_count
        self.country_code = country_code
    
    def map(movie: Movie):
        return {
                    'id': movie.id,
                    'imdb_id': movie.imdb_id,
                    'original_title': movie.original_title,
                    'release_date': movie.release_date,
                    'poster_path': movie.poster_path,
                    'vote_average': movie.vote_average,
                    'vote_count': movie.vote_count,
                    'country_code': movie.guessed_country
               }