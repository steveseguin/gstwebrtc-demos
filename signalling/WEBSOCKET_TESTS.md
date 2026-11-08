# WebSocket API compatibility tests

Run `python3 signalling/test_websocket_compat.py -v` from the repository root.
Requirements: Python 3.8+, the OpenSSL command-line tool and websockets. The
verified matrix uses Python 3.12.14 with websockets 11.0.3, 13.1, 14.2 and 16.0.
Use isolated environments for version tests, not system package changes.

websockets 14 changed the top-level `serve` alias to the new asyncio API.
The existing server constructs it before starting its event loop and uses a
two-argument handler. Both assumptions belong to the legacy protocol API.
Importing `websockets.legacy.server.serve` keeps that API explicit. A fallback
retains the top-level import for versions before the legacy namespace existed;
those historical versions have not been runtime-tested here.

The three tests cover:

- Launching the complete unmodified script in a subprocess with disposable
  certificate files and requiring its loopback TCP listener to open.
- Real plaintext WebSocket connections using the production serve expression,
  imports, argument parsing, state and handlers: HELLO, a two-peer session,
  bidirectional synthetic messages, Unicode and reconnecting after teardown.
- Repeated missing-peer errors on a connection that remains usable, plus peer
  and session cleanup when the test server closes.

The transport fixture executes the script up to certificate initialization,
then evaluates its actual `wsd` expression with an ephemeral loopback address
and `sslctx=None`. It does not replace websockets or any signaling handler.
This separates API compatibility from the independent TLS-context defect.
A successful TCP listener alone does not prove a TLS handshake. For complete
WSS operation on current Python, the server TLS-context fix in PR #2 is also
needed. Local composition with that fix passes its four standalone verified
TLS tests on every version in the matrix. The API fix alone leaves TLS unchanged.

This is a compatibility bridge, not a migration to the new asyncio API.
The legacy API and two-argument callback emit deprecation warnings on recent
versions; this does not establish support after the legacy API is removed.
Other Python versions, old websockets versions, room behavior, browser,
GStreamer, actual SDP/ICE/media, public deployment and certificate renewal
are untested. The independent session-reservation defect belongs to PR #1.
No dependency pins, security settings or trust stores are changed. Generated
test certificates and keys are removed; terminating the startup subprocess
does not establish graceful application shutdown. Choosing a free port before
startup has a small race with unrelated local processes; such failure fails
the test.
