"""Real transport compatibility tests; see WEBSOCKET_TESTS.md for limits."""
import ast
import asyncio
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import websockets


SERVER = Path(__file__).with_name('simple-server.py')


def load_server():
    source = ast.parse(SERVER.read_text(), filename=str(SERVER))
    # Preserve all imports, argument parsing, global state and actual handlers.
    # Stop before the independent certificate/TLS initialization.
    boundary = next(i for i, node in enumerate(source.body) if isinstance(
        node, ast.Assign) and any(isinstance(target, ast.Name) and
        target.id == 'certpath' for target in node.targets))
    startup = next(node.value for node in source.body if isinstance(
        node, ast.Assign) and any(isinstance(target, ast.Name) and
        target.id == 'wsd' for target in node.targets))
    source.body = source.body[:boundary]
    namespace = {'__file__': str(SERVER), '__name__': 'compat_test_server'}
    with mock.patch.object(sys, 'argv', [str(SERVER)]):
        exec(compile(source, str(SERVER), 'exec'), namespace)
    namespace.update(ADDR_PORT=('127.0.0.1', 0), sslctx=None)
    return namespace, compile(ast.Expression(startup), str(SERVER), 'eval')


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.namespace, startup = load_server()
        # Evaluate the production serve call, including its queue limit. Only
        # TLS and the address are replaced for this plaintext transport test.
        self.server = await eval(startup, self.namespace)
        self.uri = 'ws://127.0.0.1:{}'.format(
            self.server.sockets[0].getsockname()[1])

    async def asyncTearDown(self):
        self.server.close()
        await asyncio.wait_for(self.server.wait_closed(), 3)
        self.assertEqual(self.namespace['peers'], {})
        self.assertEqual(self.namespace['sessions'], {})

    def connect(self):
        return websockets.connect(self.uri, open_timeout=2, close_timeout=1)

    async def hello(self, ws, uid):
        await ws.send('HELLO ' + uid)
        self.assertEqual(await asyncio.wait_for(ws.recv(), 2), 'HELLO')

    async def test_session_messages_and_reconnect(self):
        async with self.connect() as caller, self.connect() as callee:
            await self.hello(caller, 'caller')
            await self.hello(callee, 'callee')
            await caller.send('SESSION callee')
            self.assertEqual(await asyncio.wait_for(caller.recv(), 2), 'SESSION_OK')
            for sender, receiver, payload in [
                (caller, callee, '{"sdp":"synthetic offer"}'),
                (callee, caller, '{"ice":"synthetic answer λ"}'),
            ]:
                await sender.send(payload)
                self.assertEqual(await asyncio.wait_for(receiver.recv(), 2), payload)
        async with self.connect() as ws:
            await self.hello(ws, 'after-session')

    async def test_unknown_callee_keeps_connection_usable(self):
        async with self.connect() as ws:
            await self.hello(ws, 'caller')
            for _ in range(2):
                await ws.send('SESSION missing')
                self.assertEqual(await asyncio.wait_for(ws.recv(), 2),
                                 "ERROR peer 'missing' not found")


class StartupTests(unittest.TestCase):
    def test_complete_script_reaches_listener(self):
        # This proves loop/API startup only. A TCP listener is not evidence of
        # a TLS handshake; the separate TLS-context defect needs its own fix.
        with tempfile.TemporaryDirectory(prefix='gst-api-test-') as temp:
            subprocess.run([
                'openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                '-keyout', str(Path(temp) / 'key.pem'),
                '-out', str(Path(temp) / 'cert.pem'), '-days', '1',
                '-subj', '/CN=localhost',
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            with tempfile.TemporaryFile(mode='w+') as log:
                process = subprocess.Popen([
                    sys.executable, '-u', str(SERVER), '--addr', '127.0.0.1',
                    '--port', str(port), '--cert-path', temp,
                ], stdout=log, stderr=subprocess.STDOUT)
                try:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        if process.poll() is not None:
                            log.seek(0)
                            self.fail('Server exited before listening:\n' + log.read())
                        try:
                            with socket.create_connection(('127.0.0.1', port), 0.1):
                                return
                        except OSError:
                            time.sleep(0.05)
                    self.fail('Server did not listen within five seconds')
                finally:
                    if process.poll() is None:
                        process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)


if __name__ == '__main__':
    unittest.main()
