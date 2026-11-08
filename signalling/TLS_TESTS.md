# Standalone TLS regression tests

Run `python3 signalling/test_tls_startup.py -v` from the repository root.

Requirements: Python 3.8+, the OpenSSL command-line tool with `req -addext`, and
an installed websockets version compatible with the server's existing API.
The tests were verified with Python 3.12 and websockets 13.1. They deliberately
launch the complete server without replacing its handlers, TLS context or
transport. websockets 14+ changes the default server API; this TLS fix does not
migrate the application to that API, and websockets 16 still fails at startup.
Use an isolated environment for dependency-version testing, not system packages.

The tests create disposable localhost certificates and ephemeral loopback
listeners. No certificate is installed into a trust store. Client connections
require both certificate-chain and hostname verification. Coverage includes:

- Ordinary `cert.pem`/`key.pem` and `fullchain.pem`/`privkey.pem` filename paths.
- TLS handshake, WebSocket upgrade, HELLO, two-way synthetic session messages,
  and a new connection after session teardown.
- Rejection of an untrusted certificate and a wrong hostname, followed by a
  successful verified connection.
- The existing missing-certificate error.

Each server subprocess is terminated after its test. This does not qualify
application shutdown, certificate renewal, public deployment, browser,
GStreamer, WebRTC, SDP/ICE correctness or media transport. The synthetic
`letsencrypt-example` directory checks filename selection only, not an actual
certificate authority. Selecting a free port before subprocess startup has a
small race with unrelated local processes; a startup failure fails the test.
