@presence @timeout
Feature: Presence Timeouts
	Whenever a subscribe or unsubscribe event does
	not recieve an acknowledgement from the server
	the client should emit an ack timeout error so
	that the client can attempt to retry.

Scenario: Events Timeouts
	# The client is connected
	Given the test server is ready
		And the client is initialised
		And the server sends the message C|A+
		And the client logs in with username "XXX" and password "YYY"
		And the server sends the message A|A+

	# The client subscribes to an event
	Given the client subscribes to presence events
	Then the server received the message U|S|S+

	# The server does not respond in time with a subscribe ACK
	Given some time passes
	Then the client throws a "ACK_TIMEOUT" error with message "No ACK message received in time for U"

	# The client unsubscribes from an event
	When the client unsubscribes to presence events
	Then the server received the message U|US|US+

	# The server does not respond in time with an unsubscribe ACK
	Given some time passes
	Then the client throws a "ACK_TIMEOUT" error with message "No ACK message received in time for U"
