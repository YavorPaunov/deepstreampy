from __future__ import absolute_import, division, print_function, with_statement
import sys
import inspect


PING = 'PI'
PONG = 'PO'
ACK = 'A'
REDIRECT = 'RED'
CHALLENGE = 'CH'
CHALLENGE_RESPONSE = 'CHR'
READ = 'R'
CREATE = 'C'
UPDATE = 'U'
PATCH = 'P'
DELETE = 'D'
SUBSCRIBE = 'S'
UNSUBSCRIBE = 'US'
HAS = 'H'
SNAPSHOT = 'SN'
INVOKE = 'I'
SUBSCRIPTION_FOR_PATTERN_FOUND = 'SP'
SUBSCRIPTION_FOR_PATTERN_REMOVED = 'SR'
SUBSCRIPTION_HAS_PROVIDER = 'SH'
LISTEN = 'L'
UNLISTEN = 'UL'
LISTEN_ACCEPT = 'LA'
LISTEN_REJECT = 'LR'
PROVIDER_UPDATE = 'PU'
QUERY = 'Q'
CREATEORREAD = 'CR'
EVENT = 'EVT'
ERROR = 'E'
REQUEST = 'REQ'
RESPONSE = 'RES'
REJECTION = 'REJ'
PRESENCE_JOIN = 'PNJ'
PRESENCE_LEAVE = 'PNL'
QUERY = 'Q'


_current_module = sys.modules[__name__]

_reverse_lookup_map = dict()
for name, value in inspect.getmembers(_current_module):
    if name.isupper():
        _reverse_lookup_map[value] = name


def reverse_lookup(action):
    return _reverse_lookup_map.get(action, None)
