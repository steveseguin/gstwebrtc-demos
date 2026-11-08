# Room departure regressions

From the repository root, with Python 3.8+ and an installed `websockets`
providing its legacy server namespace:

```console
python -m unittest discover -s signalling -p test_room_cleanup.py -v
```

Five deterministic async cases execute the production cleanup functions with
controlled transports. They cover normal/idempotent removal, normal and
abnormal closed recipients, membership changes while sending is suspended,
a recipient reconnecting in another room, and propagation of unexpected
errors. Only `websockets.ConnectionClosed` is treated as an expected departure.

Three real-loopback WebSocket cases use the production HELLO/room handlers
and state. They cover simultaneous departures with a surviving member,
reconnecting under a departed ID, bidirectional Unicode relay afterward,
all members leaving across two rooms, and abrupt transport losses. Final
cleanup checks empty peer/session state and room membership and rejects
server error logs. These cases deliberately do not serialize departures.

The loader preserves imports, argument parsing, globals and handler bodies,
but stops before certificate/listener startup. The transport fixture explicitly
uses `websockets.legacy.server.serve`. Thus the cleanup fix remains independent
of the separate TLS-context, startup API, session-reservation and room-list
fixes. It does not repair standalone WSS startup or peer listing by itself.

Validation uses Linux/Python 3.12 and websockets 11.0.3, 13.1, 14.2 and 16.0.
Legacy API deprecation warnings are expected on recent versions. No dependency
is changed. The tests do not qualify room joining races, every reconnect
interleaving, indefinitely blocked sends, general graceful application shutdown,
historical Python, browser/GStreamer/media, actual SDP/ICE negotiation,
certificate renewal, or deployed service operation.
