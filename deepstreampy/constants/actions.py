from __future__ import absolute_import, division, print_function, with_statement

ACK = 'A'
READ = 'R'
CREATE = 'C'
UPDATE = 'U'
PATCH = 'P'
DELETE = 'D'
SUBSCRIBE = 'S'
UNSUBSCRIBE = 'US'
INVOKE = 'I'
SUBSCRIPTION_FOR_PATTERN_FOUND = 'SP'
SUBSCRIPTION_FOR_PATTERN_REMOVED = 'SR'
LISTEN = 'L'
UNLISTEN = 'UL'
PROVIDER_UPDATE = 'PU'
QUERY = 'Q'
CREATEORREAD = 'CR'
EVENT = 'EVT'
ERROR = 'E'
REQUEST = 'REQ'
RESPONSE = 'RES'
REJECTION = 'REJ'

# WebRtc
WEBRTC_REGISTER_CALLEE = 'RC'
WEBRTC_UNREGISTER_CALLEE = 'URC'
WEBRTC_OFFER = 'OF'
WEBRTC_ANSWER = 'AN'
WEBRTC_ICE_CANDIDATE = 'IC'
WEBRTC_CALL_DECLINED = 'CD'
WEBRTC_CALL_ENDED = 'CE'
WEBRTC_LISTEN_FOR_CALLEES = 'LC'
WEBRTC_UNLISTEN_FOR_CALLEES = 'ULC'
WEBRTC_ALL_CALLEES = 'WAC'
WEBRTC_CALLEE_ADDED = 'WCA'
WEBRTC_CALLEE_REMOVED = 'WCR'
WEBRTC_IS_ALIVE = 'WIA'

import sys
import inspect
_current_module = sys.modules[__name__]

_reverse_lookup_map = dict()
for name, value in inspect.getmembers(_current_module):
    if name.isupper():
        _reverse_lookup_map[value] = name

def reverse_lookup(action):
    return _reverse_lookup_map.get(action, None)
