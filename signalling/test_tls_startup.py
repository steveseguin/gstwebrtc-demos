"""Real standalone TLS tests; see TLS_TESTS.md for runtime requirements."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import unittest

import websockets


SERVER = Path(__file__).with_name('simple-server.py')


class TLSStartupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='gst-tls-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.certdir = self.root / 'ordinary'
        self.certdir.mkdir()
        subprocess.run([
            'openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
            '-keyout', str(self.certdir / 'key.pem'),
            '-out', str(self.certdir / 'cert.pem'), '-days', '1',
            '-subj', '/CN=localhost',
            '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.context = ssl.create_default_context(
            cafile=str(self.certdir / 'cert.pem'))
        self.assertTrue(self.context.check_hostname)
        self.assertEqual(self.context.verify_mode, ssl.CERT_REQUIRED)

    @asynccontextmanager
    async def server(self, certdir):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        with tempfile.TemporaryFile(mode='w+') as log:
            process = subprocess.Popen([
                sys.executable, '-u', str(SERVER), '--addr', '127.0.0.1',
                '--port', str(port), '--cert-path', str(certdir),
            ], stdout=log, stderr=subprocess.STDOUT)
            try:
                # TCP readiness only. The tests below require a verified TLS
                # handshake, WebSocket upgrade and actual signaling messages.
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        log.seek(0)
                        self.fail('Server exited before listening:\n' + log.read())
                    try:
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection('127.0.0.1', port), 0.2)
                    except (OSError, asyncio.TimeoutError):
                        await asyncio.sleep(0.05)
                    else:
                        writer.close()
                        await writer.wait_closed()
                        break
                else:
                    self.fail('Server did not listen within five seconds')
                yield 'wss://127.0.0.1:{}'.format(port)
                self.assertIsNone(process.poll(), 'Server exited during test')
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)

    def connect(self, uri, context=None, hostname='localhost'):
        return websockets.connect(
            uri, ssl=self.context if context is None else context,
            server_hostname=hostname, open_timeout=2, close_timeout=1)

    async def hello(self, ws, uid):
        await ws.send('HELLO ' + uid)
        self.assertEqual(await asyncio.wait_for(ws.recv(), 2), 'HELLO')
        self.assertIsNotNone(ws.transport.get_extra_info('ssl_object').version())

    async def exchange(self, uri):
        async with self.connect(uri) as caller, self.connect(uri) as callee:
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
        # A fresh peer can still register after session teardown.
        async with self.connect(uri) as ws:
            await self.hello(ws, 'after-session')

    async def test_ordinary_certificate_and_session(self):
        async with self.server(self.certdir) as uri:
            await self.exchange(uri)

    async def test_letsencrypt_filenames_and_session(self):
        certdir = self.root / 'letsencrypt-example'
        certdir.mkdir()
        shutil.copyfile(self.certdir / 'cert.pem', certdir / 'fullchain.pem')
        shutil.copyfile(self.certdir / 'key.pem', certdir / 'privkey.pem')
        async with self.server(certdir) as uri:
            await self.exchange(uri)

    async def test_clients_still_verify_certificates_and_hostname(self):
        async with self.server(self.certdir) as uri:
            with self.assertRaises(ssl.SSLCertVerificationError):
                async with self.connect(uri, hostname='wrong.example'):
                    self.fail('Wrong hostname was accepted')
            with self.assertRaises(ssl.SSLCertVerificationError):
                async with self.connect(uri, context=ssl.create_default_context()):
                    self.fail('Untrusted certificate was accepted')
            async with self.connect(uri) as ws:
                await self.hello(ws, 'after-rejections')

    def test_missing_certificate_error_is_preserved(self):
        result = subprocess.run([
            sys.executable, str(SERVER), '--addr', '127.0.0.1',
            '--port', '0', '--cert-path', str(self.root / 'missing'),
        ], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 1)
        self.assertIn('Certificates not found', result.stdout)


if __name__ == '__main__':
    unittest.main()
