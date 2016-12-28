@presence
Feature: Presence
	Presence is deepstreams way of querying for clients
	and knowing about client login/logout events

Scenario: Querying

	# The client is connected
	Given the test server is ready
		And the client is initialised
		And the server sends the message C|A+
		And the client logs in with username "XXX" and password "YYY"
		And the server sends the message A|A+

	# Happy Path
	# The client queries for clients when none connected
	Given the client queries for connected clients
	Then the server received the message U|Q|Q+

	When the server sends the message U|Q+
	Then the client is notified that no clients are connected

	# Client queries for clients with multiple connectd
	Given the client queries for connected clients
	Then the server received the message U|Q|Q+

	When the server sends the message U|Q|Homer|Marge|Bart+
	Then the client is notified that clients "Homer,Marge,Bart" are connected
