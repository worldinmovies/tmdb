Feature: IMDB Imports

  Background:
    Given basics are present in mongo

  Scenario: Import Ratings Happy Case
    Given movies "[{"id": 1, "imdb_id": "tt0000001"}]" is persisted
    And "https://datasets.imdbws.com/title.ratings.tsv.gz" is zip-mocked with "mini_ratings.tsv"
    When calling /import/imdb/ratings
    Then http status should be 200
    And imdb_id=tt0000001 should have imdb_ratings set to 5.8 eventually

  Scenario: Import Ratings Rerun should ignore same values
    Given movies "[{"id": 1, "imdb_id": "tt0000001"}]" is persisted
    And "https://datasets.imdbws.com/title.ratings.tsv.gz" is zip-mocked with "mini_ratings.tsv"
    And calling /import/imdb/ratings
    And http status should be 200
    And imdb_id=tt0000001 should have imdb_ratings set to 5.8 eventually
    And "https://datasets.imdbws.com/title.ratings.tsv.gz" is zip-mocked with "mini_ratings2.tsv"
    When calling /import/imdb/ratings
    Then http status should be 200
    And imdb_id=tt0000001 should have imdb_ratings set to 0.1 eventually

  Scenario: Import Titles Happy Case
    Given movies "[{"id": 1, "imdb_id": "tt0000001"}]" is persisted
    And "https://datasets.imdbws.com/title.akas.tsv.gz" is zip-mocked with "mini_akas.tsv"
    When calling /import/imdb/titles
    Then http status should be 200
    And imdb_id=tt0000001 should have imdb_alt_titles "Carmencita - spanyol tánc,Καρμενσίτα,Карменсита" set eventually

  Scenario: Import Titles Rerun should ignore same values
    Given movies "[{"id": 1, "imdb_id": "tt0000001"}]" is persisted
    And "https://datasets.imdbws.com/title.akas.tsv.gz" is zip-mocked with "mini_akas.tsv"
    And calling /import/imdb/titles
    And http status should be 200
    And imdb_id=tt0000001 should have imdb_alt_titles "Carmencita - spanyol tánc,Καρμενσίτα,Карменсита" set eventually
    And "https://datasets.imdbws.com/title.akas.tsv.gz" is zip-mocked with "mini_akas2.tsv"
    When calling /import/imdb/titles
    Then http status should be 200
    And imdb_id=tt0000001 should have imdb_alt_titles "Carmencita,Καρμενσίτα,Карменсита" set eventually
