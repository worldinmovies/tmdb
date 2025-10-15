from app import views
from django.urls import path, re_path, include

urlpatterns = [
    # Imports a daily file with the data of what movies are available to download
    path('import/tmdb/daily',               views.download_file),
    # Starts to fetch info from tmdb with the keys from daily
    path('import/tmdb/data',                views.import_tmdb_data),
    # Runs /daily, /genres, /countries, /languages
    path('import/base',                     views.base_fetch),
    path('import/tmdb/genres',              views.fetch_genres),
    path('import/tmdb/countries',           views.fetch_countries),
    path('import/tmdb/languages',           views.fetch_languages),
    path('import/tmdb/providers',           views.fetch_providers),
    path('import/tmdb/changes',             views.check_tmdb_for_changes),

    path('index/movies',                    views.index_meilisearch),
    path('search/movies/<str:query>',                   views.search_movies),

    # IMDB
    path('import/imdb/ratings',             views.fetch_imdb_ratings),
    path('import/imdb/titles',              views.fetch_imdb_titles),
    path('imdb/ratings',                    views.ratings),

    path('dump/genres',                     views.dump_genres),
    path('dump/langs',                      views.dump_langs),
    path('dump/countries',                  views.dump_countries),
    path('redo/guestimation',               views.redo_guestimation),
    path('view/best/<str:country_code>',    views.get_best_movies_from_country),
    path('view/random/<str:country_code>',  views.get_random_movies_by_country),
    path('view/random/best/<int:movies>',   views.get_best_randoms),
    path('movies/<str:ids>',                views.fetch_movies_data),
    path('movie/<str:id>',                  views.fetch_movie_data),

    path('redo/populatediscovery',          views.populate_discovery),
    re_path(r'^status$',                    views.import_status),
    re_path(r'^health/',                    include('health_check.urls'))  # Should remove to mitigate ddos-risk
]


