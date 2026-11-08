"""Room departure regressions; see ROOM_CLEANUP_TESTS.md for scope."""
import ast
import asyncio
from contextlib import AsyncExitStack
import logging
from pathlib import Path
import sys
import unittest
from unittest import mock

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.legacy.server import serve


SERVER = Path(__file__).with_name('simple-server.py')


def load_server():
    source = ast.parse(SERVER.read_text(), filename=str(SERVER))
    boundary = next(i for i, node in enumerate(source.body) if isinstance(
        node, ast.Assign) and any(isinstance(target, ast.Name) and
        target.id == 'certpath' for target in node.targets))
    source.body = source.body[:boundary]
    namespace = {'__file__': str(SERVER), '__name__': 'cleanup_test_server'}
    with mock.patch.object(sys, 'argv', [str(SERVER)]):
        exec(compile(source, str(SERVER), 'exec'), namespace)
    return namespace


class Peer:
    def __init__(self, send=None):
        self.sent = []
        self.closed = False
        self.on_send = send

    async def send(self, message):
        if self.on_send:
            await self.on_send(message)
        self.sent.append(message)

    async def close(self):
        self.closed = True


class CleanupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.namespace = load_server()
        self.peers = self.namespace['peers']
        self.rooms = self.namespace['rooms']
        self.remove = self.namespace['remove_peer']

    def room(self, *names):
        self.rooms['room'] = set(names)
        for name in names:
            self.peers[name] = [Peer(), ('test', 0), 'room']

    async def test_normal_departure_is_idempotent(self):
        self.room('alice', 'bob')
        alice, bob = self.peers['alice'][0], self.peers['bob'][0]
        await self.remove('alice')
        await self.remove('alice')
        self.assertEqual(bob.sent, ['ROOM_PEER_LEFT alice'])
        self.assertTrue(alice.closed)
        self.assertEqual(set(self.peers), {'bob'})
        self.assertEqual(self.rooms['room'], {'bob'})

    async def test_closed_recipient_does_not_block_cleanup_or_other_notices(self):
        for exception in (ConnectionClosedOK, ConnectionClosedError):
            with self.subTest(exception=exception.__name__):
                self.setUp()
                self.room('alice', 'bob', 'carol', 'dave')
                alice = self.peers['alice'][0]
                closed_id = next(uid for uid in self.rooms['room'] if uid != 'alice')
                async def closed(message):
                    raise exception(None, None)
                self.peers[closed_id][0].on_send = closed
                await self.remove('alice')
                self.assertNotIn('alice', self.peers)
                self.assertEqual(self.rooms['room'], {'bob', 'carol', 'dave'})
                self.assertTrue(alice.closed)
                for name in {'bob', 'carol', 'dave'} - {closed_id}:
                    self.assertEqual(self.peers[name][0].sent,
                                     ['ROOM_PEER_LEFT alice'])

    async def test_membership_can_change_while_notice_is_blocked(self):
        self.room('alice', 'bob', 'carol', 'dave')
        # Pick the real iteration order so this case is hash-seed independent.
        order = tuple(uid for uid in self.rooms['room'] if uid != 'alice')
        first, departing = order[:2]
        entered, release = asyncio.Event(), asyncio.Event()
        async def blocked(message):
            if message == 'ROOM_PEER_LEFT alice':
                entered.set()
                await release.wait()
        self.peers[first][0].on_send = blocked
        task = asyncio.create_task(self.remove('alice'))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            await self.remove(departing)
            self.peers['newcomer'] = [Peer(), ('test', 0), 'room']
            self.rooms['room'].add('newcomer')
        finally:
            release.set()
            await asyncio.wait_for(task, 2)
        self.assertEqual(set(self.peers), {first, order[2], 'newcomer'})
        self.assertEqual(self.rooms['room'], set(self.peers))
        self.assertEqual(self.peers['newcomer'][0].sent, [])
        self.assertIn('ROOM_PEER_LEFT alice', self.peers[order[2]][0].sent)

    async def test_reconnected_recipient_in_another_room_is_skipped(self):
        self.room('alice', 'bob', 'carol')
        first, moving = tuple(uid for uid in self.rooms['room'] if uid != 'alice')
        entered, release = asyncio.Event(), asyncio.Event()
        async def blocked(message):
            if message == 'ROOM_PEER_LEFT alice':
                entered.set()
                await release.wait()
        self.peers[first][0].on_send = blocked
        task = asyncio.create_task(self.remove('alice'))
        replacement = Peer()
        try:
            await asyncio.wait_for(entered.wait(), 2)
            await self.remove(moving)
            self.peers[moving] = [replacement, ('test', 1), 'elsewhere']
            self.rooms['elsewhere'] = {moving}
        finally:
            release.set()
            await asyncio.wait_for(task, 2)
        self.assertEqual(replacement.sent, [])
        self.assertEqual(self.rooms['room'], {first})
        self.assertEqual(self.rooms['elsewhere'], {moving})

    async def test_unexpected_send_errors_are_not_suppressed(self):
        self.room('alice', 'bob')
        async def broken(message):
            raise ValueError('unexpected failure')
        self.peers['bob'][0].on_send = broken
        with self.assertRaisesRegex(ValueError, 'unexpected failure'):
            await self.remove('alice')


class ErrorLog(logging.Handler):
    def __init__(self):
        super().__init__(logging.ERROR)
        self.records = []

    def emit(self, record):
        self.records.append(record)


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.namespace = load_server()
        self.errors = ErrorLog()
        logger = logging.getLogger('cleanup-tests.' + self.id())
        logger.addHandler(self.errors)
        self.addCleanup(logger.removeHandler, self.errors)
        self.server = await serve(self.namespace['handler'], '127.0.0.1', 0,
                                  max_queue=16, logger=logger)
        self.clients = AsyncExitStack()
        self.uri = 'ws://127.0.0.1:{}'.format(
            self.server.sockets[0].getsockname()[1])
        self.addAsyncCleanup(self.shutdown)

    async def shutdown(self):
        try:
            await self.clients.aclose()
        finally:
            self.server.close()
            await asyncio.wait_for(self.server.wait_closed(), 3)
        self.assertEqual(self.namespace['peers'], {})
        self.assertEqual(self.namespace['sessions'], {})
        self.assertTrue(all(not peers for peers in self.namespace['rooms'].values()))
        self.assertEqual([r.getMessage() for r in self.errors.records], [])

    async def message(self, ws):
        return await asyncio.wait_for(ws.recv(), 2)

    async def peer(self, uid, room, current):
        ws = await self.clients.enter_async_context(websockets.connect(
            self.uri, open_timeout=2, close_timeout=1))
        await ws.send('HELLO ' + uid)
        self.assertEqual(await self.message(ws), 'HELLO')
        await ws.send('ROOM ' + room)
        reply = await self.message(ws)
        self.assertTrue(reply.startswith('ROOM_OK '), reply)
        self.assertEqual(set(reply[8:].split()), set(current))
        for other in current.values():
            self.assertEqual(await self.message(other), 'ROOM_PEER_JOINED ' + uid)
        current[uid] = ws
        return ws

    async def removed(self, *uids):
        async def wait():
            while any(uid in self.namespace['peers'] for uid in uids):
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), 2)

    async def test_simultaneous_departures_leave_survivor_usable_and_ids_reusable(self):
        current = {}
        for uid in ('alice', 'bob', 'carol', 'dave'):
            await self.peer(uid, 'room', current)
        survivor = current['alice']
        leaving = ('bob', 'carol', 'dave')
        await asyncio.gather(*(current[uid].close() for uid in leaving))
        notices = [await self.message(survivor) for _ in leaving]
        self.assertCountEqual(notices, ['ROOM_PEER_LEFT ' + uid for uid in leaving])
        await self.removed(*leaving)
        self.assertEqual(self.namespace['rooms']['room'], {'alice'})
        current = {'alice': survivor}
        bob = await self.peer('bob', 'room', current)
        await survivor.send('ROOM_PEER_MSG bob hello λ')
        self.assertEqual(await self.message(bob), 'ROOM_PEER_MSG alice hello λ')
        await bob.send('ROOM_PEER_MSG alice still usable')
        self.assertEqual(await self.message(survivor), 'ROOM_PEER_MSG bob still usable')

    async def test_all_members_can_leave_together_in_multiple_rooms(self):
        clients = []
        for room in ('one', 'two'):
            current = {}
            for n in range(5):
                clients.append(await self.peer(room + str(n), room, current))
        await asyncio.gather(*(ws.close() for ws in clients))
        await self.removed(*(room + str(n) for room in ('one', 'two') for n in range(5)))
        self.assertTrue(all(not peers for peers in self.namespace['rooms'].values()))

    async def test_abrupt_departures_do_not_prevent_survivor_notifications(self):
        current = {}
        for uid in ('alice', 'bob', 'carol'):
            await self.peer(uid, 'room', current)
        for uid in ('bob', 'carol'):
            current[uid].transport.abort()
        notices = [await self.message(current['alice']) for _ in range(2)]
        self.assertCountEqual(notices, ['ROOM_PEER_LEFT bob', 'ROOM_PEER_LEFT carol'])
        await self.removed('bob', 'carol')
        self.assertEqual(self.namespace['rooms']['room'], {'alice'})


if __name__ == '__main__':
    unittest.main()
