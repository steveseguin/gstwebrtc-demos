# Room-list regressions

From the repository root, using Python 3.8+ and an installed `websockets` with
its legacy server namespace (tested with 11.0.3, 13.1, 14.2 and 16.0):

```console
python -m unittest discover -s signalling -p test_rooms.py -v
```

The five cases use real loopback WebSockets, the production HELLO/room
handlers and state, and actual client messages. They cover repeated empty
lists, self-exclusion, multiple peers, room isolation, membership after
leave/rejoin, Unicode message forwarding after listing, and two non-listing
controls. List order is intentionally unspecified.

The fixture loads the production module up to its certificate setup, then
uses `websockets.legacy.server.serve` explicitly. It does not run the script's
standalone startup, TLS initialization, or event-loop setup. This keeps the
room-list regression independent of the separate server TLS-context and
WebSocket startup compatibility fixes. The fix alone does not make current
standalone WSS startup work.

Teardown waits for each server-side peer removal before closing the next
client. Rapid overlapping room departures can still expose an existing
cleanup race when a leave notification targets a closing connection. This
suite does not claim to fix or qualify simultaneous disconnect handling.

No browser, GStreamer, real SDP/ICE negotiation, media, historical Python
runtime, certificate renewal, or public deployment is exercised. Legacy API
deprecation warnings are expected on recent websockets versions.
