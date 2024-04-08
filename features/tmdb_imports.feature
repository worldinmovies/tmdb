Feature: TMDB Imports

  Background:
    Given basics are present in mongo

  Scenario Outline: Daily Import
    Given movies "<json>" is persisted
    And tmdb file is mocked with movie_ids.json.gz
    When calling /import/tmdb/daily
    Then http status should be 200
    And after awhile there should be <amount> movies persisted

    Examples: Happy Cases
      | json                                                        | amount |
      | []                                                          | 3      |
      | [{"id": 604, "fetched": false},{"id": 605, "fetched":true}] | 4      |

  Scenario Outline: Data Import
    Given movies "<json>" is persisted
    And tmdb data is mocked with <mocked_data> for id <id> with status <status>
    When calling /import/tmdb/data
    Then http status should be 200
    And after awhile there should be <amount> movies persisted

    Examples: Happy Cases
      | json                                                         | mocked_data        | id    | status | amount |
      | [{"id": 601, "fetched": false}]                              | 601.json           | 601   | 200    | 1      |
      | [{"id": 601, "fetched": false},{"id": 602, "fetched": true}] | 601.json           | 601   | 200    | 2      |
      | [{"id": 601, "fetched": false}]                              | 601.json           | 601   | 200    | 1      |
      | [{"id": 19995, "fetched": false}]                            | failing_movie.json | 19995 | 200    | 1      |
      #| [{"id": 123, "fetched": false}]                              | 601.json           | 123   | 404    | 0      |


  Scenario Outline: Base Import
    Given base data <mock_url> is mocked with <mocked_data>
    And basics are removed from mongo
    When calling /import/tmdb/<path>
    Then http status should be 200
    And there should be <countries> countries persisted
    And there should be <languages> languages persisted
    And there should be <genres> genres persisted

    Examples: Happy Cases
      | mock_url                                      | path      | mocked_data    | countries | languages | genres |
      | /configuration/countries?api_key=test         | countries | countries.json | 247       | 0         | 0      |
      | /configuration/languages?api_key=test         | languages | languages.json | 0         | 187       | 0      |
      | /genre/movie/list?api_key=test&language=en-US | genres    | genres.json    | 0         | 0         | 19     |

  Scenario: All Base Imports
    Given base data /configuration/countries?api_key=test is mocked with countries.json
    And base data /configuration/languages?api_key=test is mocked with languages.json
    And base data /genre/movie/list?api_key=test&language=en-US is mocked with genres.json
    And tmdb file is mocked with movie_ids.json.gz
    And basics are removed from mongo
    When calling /import/base
    Then http status should be 200
    And after awhile there should be 3 movies persisted
    And there should be 247 countries persisted
    And there should be 187 languages persisted
    And there should be 19 genres persisted


  Scenario Outline: Check TMDB For Changes
    Given movies "[{"id": 578908, "fetched": true, "fetched_date": "2018-01-01"}]" is persisted
    And base data /movie/changes?api_key=test&start_date=2019-01-01&end_date=2019-01-02&page=1 is mocked with tmdb_changes.json
    When calling /import/tmdb/changes?start_date=2019-01-01&end_date=2019-01-02
    Then http status should be 200
    And after awhile there should be <amount> movies persisted
    And <id> should have "fetched" set to "False"

    Examples:
      | amount | id     |
      | 1      | 578908 |
