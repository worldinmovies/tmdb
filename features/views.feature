Feature: Views

  Background:
    Given basics are present in mongo


  Scenario Outline: View Best Endpoint2
    Given movies from file:<file> is persisted
    When calling <url>
    Then http status should be 200
    And response should contain "<expected>"

    Examples: /view/best endpoint
      | file                 | url           | expected            |
      | sjunde_inseglet.json | /view/best/SE | Det sjunde inseglet |
      | 1398.json            | /view/best/SU | "title": "Stalker"  |

    Examples: /view/random/best endpoint
      | file                 | url                              | expected            |
      | sjunde_inseglet.json | /view/random/best/0?countries=SE | Det sjunde inseglet |
      | 1398.json            | /view/random/best/0?countries=SU | tt0079944  |
