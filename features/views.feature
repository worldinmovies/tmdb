Feature: Views

  Background:
    Given basics are present in mongo

  Scenario Outline: Status Endpoint
    Given movies "<json>" is persisted
    When calling /status
    Then http status should be 200
    And response should be <expected_response>

    Examples: /status endpoint
      | json                                    | expected_response                                   |
      | [{"id": 1}]                             | {"total": 1, "fetched": 0, "percentageDone": 0.0}   |
      | [{"id": 1, "fetched": true}]            | {"total": 1, "fetched": 1, "percentageDone": 100.0} |
      | [{"id": 1}, {"id": 2}]                  | {"total": 2, "fetched": 0, "percentageDone": 0.0}   |
      | [{"id": 1, "fetched": true}, {"id": 2}] | {"total": 2, "fetched": 1, "percentageDone": 50.0}  |

  Scenario Outline: View Best Endpoint
    Given movies from file:<file> is persisted
    When calling <url>
    Then http status should be 200
    And response should contain "<expected>"

    Examples: /view/best endpoint
      | file                 | url           | expected            |
      | sjunde_inseglet.json | /view/best/SE | Det sjunde inseglet |
      | 1398.json            | /view/best/SU | "title": "Stalker"  |

    # Doesn't support $firstN as a group operator in mongomock
    Examples: /view/random/best endpoint
      | file | url | http_status | expected |
      #| testdata/sjunde_inseglet.json | /view/random/best/0 | 200         | Det sjunde inseglet |
      #| testdata/1398.json            | /view/random/best/1 | 200         | "title": "Stalker"  |
