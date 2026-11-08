"""Session routing regressions; run with python3 -m unittest discover -s signalling.

Load the server's real handlers without its command-line/TLS startup. The
in-memory sockets keep send scheduling deterministic, including a blocked ACK.
"""
import ast
import asyncio
from pathlib import Path
import unittest


def load_server():
    path = Path(__file__).with_name('simple-server.py')
    source = ast.parse(path.read_text(), filename=str(path))
    source.body = [node for node in source.body if isinstance(
        node, (ast.Import, ast.ImportFrom, ast.AsyncFunctionDef))]
    server = {'peers': {}, 'sessions': {}, 'rooms': {},
              'pending_room_joins': set(), 'KEEPALIVE_TIMEOUT': 30}
    exec(compile(source, str(path), 'exec'), server)
    return server


class Socket:
    def __init__(self, name, ack_gate=None):
        self.remote_address = name
        self.incoming = asyncio.Queue()
        self.sent = asyncio.Queue()
        self.ack_gate = ack_gate

    async def recv(self):
        return await self.incoming.get()

    async def send(self, message):
        await self.sent.put(message)
        if message == 'SESSION_OK' and self.ack_gate is not None:
            await self.ack_gate.wait()


class RoomSocket(Socket):
    def __init__(self, name, gate, error=None):
        super().__init__(name)
        self.gate = gate
        self.error = error

    async def send(self, message):
        await super().send(message)
        if message.startswith('ROOM_OK '):
            await self.gate.wait()
            if self.error is not None:
                raise self.error

    async def close(self):
        pass


class SessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.server = load_server()
        self.tasks = []

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def add_peer(self, uid, status=None):
        socket = Socket(uid)
        self.server['peers'][uid] = [socket, uid, status]
        return socket

    def call(self, uid, callee, ack_gate=None):
        socket = Socket(uid, ack_gate)
        socket.incoming.put_nowait('SESSION ' + callee)
        self.tasks.append(asyncio.create_task(
            self.server['connection_handler'](socket, uid)))
        return socket

    async def message(self, socket):
        return await asyncio.wait_for(socket.sent.get(), 1)

    async def test_available_peer_can_receive_a_call_and_messages(self):
        callee = self.add_peer('callee')
        caller = self.call('caller', 'callee')
        self.assertEqual(await self.message(caller), 'SESSION_OK')
        self.assertEqual(self.server['sessions'],
                         {'caller': 'callee', 'callee': 'caller'})
        caller.incoming.put_nowait('offer')
        self.assertEqual(await self.message(callee), 'offer')

    async def test_busy_session_is_not_replaced(self):
        self.add_peer('first', 'session')
        self.add_peer('callee', 'session')
        original = {'first': 'callee', 'callee': 'first'}
        self.server['sessions'].update(original)
        caller = self.call('new', 'callee')
        self.assertEqual(await self.message(caller), "ERROR peer 'callee' busy")
        self.assertEqual(self.server['sessions'], original)
        self.assertIsNone(self.server['peers']['new'][2])

    async def test_room_member_cannot_be_taken_into_a_session(self):
        self.add_peer('callee', 'conference')
        self.server['rooms']['conference'] = {'callee'}
        caller = self.call('new', 'callee')
        self.assertEqual(await self.message(caller), "ERROR peer 'callee' busy")
        self.assertEqual(self.server['peers']['callee'][2], 'conference')
        self.assertEqual(self.server['rooms'], {'conference': {'callee'}})
        self.assertEqual(self.server['sessions'], {})

    async def test_callee_is_reserved_before_session_ack_can_yield(self):
        self.add_peer('callee')
        gate = asyncio.Event()
        first = self.call('first', 'callee', gate)
        self.assertEqual(await self.message(first), 'SESSION_OK')
        second = self.call('second', 'callee')
        self.assertEqual(await self.message(second), "ERROR peer 'callee' busy")
        self.assertEqual(self.server['sessions'],
                         {'first': 'callee', 'callee': 'first'})
        self.assertIsNone(self.server['peers']['second'][2])
        gate.set()

    async def test_unknown_callee_is_rejected_without_creating_a_session(self):
        caller = self.call('caller', 'missing')
        self.assertEqual(await self.message(caller), "ERROR peer 'missing' not found")
        self.assertEqual(self.server['sessions'], {})
        self.assertIsNone(self.server['peers']['caller'][2])


    def join_room(self, uid, gate, error=None):
        socket = RoomSocket(uid, gate, error)
        socket.incoming.put_nowait('ROOM conference')
        task = asyncio.create_task(self.server['connection_handler'](socket, uid))
        self.tasks.append(task)
        return socket, task

    async def test_pending_room_join_cannot_be_taken_into_a_session(self):
        member = self.add_peer('member', 'conference')
        self.server['rooms']['conference'] = {'member'}
        gate = asyncio.Event()
        joiner, _ = self.join_room('joiner', gate)
        self.assertEqual(await self.message(joiner), 'ROOM_OK member')
        # Preserve the original room visibility until the acknowledgement ends.
        self.assertIsNone(self.server['peers']['joiner'][2])
        self.assertEqual(self.server['rooms']['conference'], {'member'})
        self.assertTrue(member.sent.empty())
        caller = self.call('caller', 'joiner')
        self.assertEqual(await self.message(caller), "ERROR peer 'joiner' busy")
        self.assertEqual(self.server['sessions'], {})
        gate.set()
        self.assertEqual(await self.message(member), 'ROOM_PEER_JOINED joiner')
        self.assertEqual(self.server['rooms']['conference'], {'member', 'joiner'})
        self.assertEqual(self.server['pending_room_joins'], set())
        caller.incoming.put_nowait('SESSION joiner')
        self.assertEqual(await self.message(caller), "ERROR peer 'joiner' busy")

    async def test_cancelled_room_ack_releases_reservation_without_membership(self):
        joiner, task = self.join_room('joiner', asyncio.Event())
        self.assertEqual(await self.message(joiner), 'ROOM_OK ')
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.server['pending_room_joins'], set())
        self.assertEqual(self.server['rooms']['conference'], set())
        self.assertIsNone(self.server['peers']['joiner'][2])
        await self.server['remove_peer']('joiner')
        self.assertNotIn('joiner', self.server['peers'])

    async def test_failed_room_ack_releases_reservation_without_membership(self):
        from websockets.exceptions import ConnectionClosedError
        for error in (ConnectionClosedError(None, None), ValueError('send failed')):
            with self.subTest(error=type(error).__name__):
                member = self.add_peer('member', 'conference')
                self.server['rooms']['conference'] = {'member'}
                gate = asyncio.Event()
                joiner, task = self.join_room('joiner', gate, error)
                self.assertEqual(await self.message(joiner), 'ROOM_OK member')
                gate.set()
                with self.assertRaises(type(error)):
                    await task
                self.assertEqual(self.server['pending_room_joins'], set())
                self.assertEqual(self.server['rooms']['conference'], {'member'})
                self.assertTrue(member.sent.empty())
                self.assertIsNone(self.server['peers']['joiner'][2])
                await self.server['remove_peer']('joiner')
                self.assertNotIn('joiner', self.server['peers'])

    async def test_pending_room_join_does_not_block_unrelated_sessions(self):
        joiner, _ = self.join_room('joiner', asyncio.Event())
        self.assertEqual(await self.message(joiner), 'ROOM_OK ')
        callee = self.add_peer('callee')
        caller = self.call('caller', 'callee')
        self.assertEqual(await self.message(caller), 'SESSION_OK')
        caller.incoming.put_nowait('offer')
        self.assertEqual(await self.message(callee), 'offer')
        self.assertEqual(self.server['sessions'],
                         {'caller': 'callee', 'callee': 'caller'})


if __name__ == '__main__':
    unittest.main()
