Feature: Persistence Logics

  Background:
    Given all basics are present in mongo

  Scenario Outline: Guessed Country
    Given movies from file:<mocked_data> is persisted
    Then id=<id> should have country set to <guessed_country> eventually

    Examples: Normal cases
      | mocked_data          | id     | guessed_country |
      | sjunde_inseglet.json | 490    | SE              |
      # Jagten: Danish
      | 103663.json          | 103663 | DK              |
      # Borg vs McEnroe: Swedish
      | 397538.json          | 397538 | SE              |
      | godfather.json       | 238    | US              |
      | incendies.json       | 46738  | CA              |
      | lastnight.json       | 16129  | CA              |
      | clockworkorange.json | 185    | US              |

    Examples: Special Cases
      | mocked_data | id   | guessed_country |
      # Stalker: Made by Soviet
      | 1398.json   | 1398 | SU              |

  Scenario: Guessed country update
    Given movies from file:sjunde_inseglet.json is persisted
    And guessed_country field is nulled for id=490
    When calling /redo/guestimation
    Then id=490 should have country set to SE eventually
