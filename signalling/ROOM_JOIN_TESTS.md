# Room join notification checks

Run from the repository root with Python 3 and `websockets` installed:

```sh
python3 signalling/test_room_join.py -v
```

The nine tests cover ordinary joins and relay, normal/error closed recipients,
room membership changes while a notification is suspended, removed recipients,
recipients reconnected into another room, a failed initial roster send, and
propagation of unexpected exceptions. Joining peers remain connected after a
different peer closes, and remaining recipients still receive the join notice.

Seven deterministic cases load the complete production handlers and registries
but stop before certificate setup and listener startup. They use send/receive
doubles to force the interleavings. Two loopback cases use real legacy WebSocket
server/client connections and normal production handlers. Their protocol subclass
pauses the first join notification, closes an actual recipient (or another room
member), then calls the real transport send. They verify notices, bidirectional
Unicode relay, connection survival, cleanup and absence of server error logs.
This is controlled scheduling, not naturally induced network backpressure.

Tests pass on Python 3.12 with websockets 11.0.3, 13.1, 14.2 and 16.0. The fixture
uses the explicit legacy server API; it does not qualify standalone startup or
TLS. Full modern startup still requires the separate TLS/API compatibility fixes.

The change snapshots recipients after successful ROOM_OK and membership insertion,
skips recipients no longer registered in that room, and tolerates only closed
connections. It does not change when membership commits or retry notifications.
Peers joining later obtain the joining member in their own initial roster, so
they are not added to the earlier notification traversal. The actual send may
still suspend; there is no timeout or new background task.

Not covered: races during the initial ROOM_OK send, same-ID/same-room replacement
generations, cancellation of the join handler, indefinitely blocked sends, every
notification ordering, browser/GStreamer/media, actual SDP/ICE, old Python,
certificate lifecycle, production deployment or arbitrary load. An initial roster
can still become stale while its send is suspended. This is a focused repair of
the existing notification loop, not an atomic membership protocol.
