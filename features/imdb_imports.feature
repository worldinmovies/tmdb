Feature: IMDB Imports

  Background:
    Given basics are present in mongo

  Scenario Outline: Import Ratings
    Given movies "<json>" is persisted
    And https://datasets.imdbws.com/title.ratings.tsv.gz is mocked with mini_ratings.tsv.gz
    When calling /import/imdb/ratings
    Then http status should be 200
    And imdb_id=<imdb_id> should have imdb_ratings set eventually

    Examples: Happy Cases
      | json                                | imdb_id   |
      | [{"id": 1, "imdb_id": "tt0000001"}] | tt0000001 |

  Scenario Outline: Import Titles
    Given movies "<json>" is persisted
    And https://datasets.imdbws.com/title.akas.tsv.gz is mocked with title.akas.tsv.gz
    When calling /import/imdb/titles
    Then http status should be 200
    And imdb_id=<imdb_id> should have imdb_alt_titles <alt_titles> set eventually

    Examples: Happy Cases
      | json                                | imdb_id   | alt_titles                                      |
      | [{"id": 1, "imdb_id": "tt0000001"}] | tt0000001 | Carmencita - spanyol tánc,Καρμενσίτα,Карменсита |
