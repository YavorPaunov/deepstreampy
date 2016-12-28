@presence
Feature: Presence
    Presence is deepstreams way of querying for clients
    and knowing about client login/logout events

Scenario: Presence

    # The client is connected
    Given the test server is ready
        And the client is initialised
        And the server sends the message C|A+
        And the client logs in with username "XXX" and password "YYY"
        And the server sends the message A|A+

    # Happy Path
    # The client subscribes to presence events
    Given the client subscribes to presence events
    Then the server received the message U|S|S+

    # The server sends an ACK message for subscription
    Given the server sends the message U|A|S|U+

    # The the client is alerted when a client logs in
    When the server sends the message U|PNJ|Homer+
    Then the client is notified that client "Homer" logged in

    # The the client is alerted when a client logs in
    When the server sends the message U|PNL|Bart+
    Then the client is notified that client "Bart" logged out

    # Client is no longer alerted to presence events after unsubscribing
    Given the client unsubscribes to presence events
    Then the server received the message U|US|US+

    # The server sends an ACK message for unsubscription
    Given the server sends the message U|A|US|U+

    When the server sends the message U|PNJ|Homer+
    Then the client is not notified that client "Homer" logged in
