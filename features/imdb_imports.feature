Feature: IMDB Imports

  Background:
    Given basics are present in mongo

  Scenario Outline: Import Ratings
    Given movies "<json>" is persisted
    And https://datasets.imdbws.com/title.ratings.tsv.gz is mocked with mini_ratings.tsv.gz
    When calling /import/imdb/ratings
    Then http status should be 200

    Examples: Happy Cases
      | json                                                        | amount |
      #| []                                                          | 3      |

  Scenario Outline: Import Titles
    Given movies "<json>" is persisted
    And https://datasets.imdbws.com/title.akas.tsv.gz is mocked with title.akas.tsv.gz
    When calling /import/imdb/titles
    Then http status should be 200

    Examples: Happy Cases
      | json                                                        | amount |
      #| []                                                          | 3      |
