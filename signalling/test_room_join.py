"""Room join notification regressions; see ROOM_JOIN_TESTS.md for scope."""
import ast
import asyncio
import logging
from pathlib import Path
import sys
import unittest
from unittest import mock

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.legacy.server import WebSocketServerProtocol, serve


SERVER = Path(__file__).with_name('simple-server.py')


def load_server():
    source = ast.parse(SERVER.read_text(), filename=str(SERVER))
    boundary = next(i for i, node in enumerate(source.body) if isinstance(
        node, ast.Assign) and any(isinstance(target, ast.Name) and
        target.id == 'certpath' for target in node.targets))
    source.body = source.body[:boundary]
    namespace = {'__file__': str(SERVER), '__name__': 'join_test_server'}
    with mock.patch.object(sys, 'argv', [str(SERVER)]):
        exec(compile(source, str(SERVER), 'exec'), namespace)
    return namespace


class Peer:
    def __init__(self, send=None):
        self.sent = []
        self.on_send = send
        self.incoming = asyncio.Queue()
        self.remote_address = ('test', 0)
        self.waiting = asyncio.Event()

    async def recv(self):
        self.waiting.set()
        return await self.incoming.get()

    async def send(self, message):
        if self.on_send:
            await self.on_send(message)
        self.sent.append(message)


class RoomJoinTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.namespace = load_server()
        self.peers = self.namespace['peers']
        self.rooms = self.namespace['rooms']
        self.tasks = []

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def room(self, *names):
        self.rooms['room'] = set(names)
        for name in names:
            self.peers[name] = [Peer(), ('test', 0), 'room']

    def joining(self, send=None):
        ws = Peer(send)
        ws.incoming.put_nowait('ROOM room')
        task = asyncio.create_task(self.namespace['connection_handler'](ws, 'new'))
        self.tasks.append(task)
        return ws, task

    async def ready(self, ws, task):
        # Once the command loop asks for its second message, joining finished.
        async def wait():
            while not (ws.sent and ws.waiting.is_set() and ws.incoming.empty()):
                if task.done():
                    await task
                await asyncio.sleep(0)
            # The event also marks the first receive. Check a relay roundtrip
            # to prove the handler has finished notification and remains alive.
            ws.incoming.put_nowait('invalid command')
            while 'ERROR invalid msg, already in room' not in ws.sent:
                if task.done():
                    await task
                await asyncio.sleep(0)
        await asyncio.wait_for(wait(), 2)
        self.assertFalse(task.done())
        self.assertEqual(self.peers['new'][2], 'room')
        self.assertIn('new', self.rooms['room'])

    async def test_normal_join_lists_existing_peers_and_notifies_each_once(self):
        self.room('alice', 'bob')
        ws, task = self.joining()
        await self.ready(ws, task)
        self.assertEqual(set(ws.sent[0][8:].split()), {'alice', 'bob'})
        for uid in ('alice', 'bob'):
            self.assertEqual(self.peers[uid][0].sent, ['ROOM_PEER_JOINED new'])
        ws.incoming.put_nowait('ROOM_PEER_MSG alice hello λ')
        async def delivered():
            while len(self.peers['alice'][0].sent) < 2:
                await asyncio.sleep(0)
        await asyncio.wait_for(delivered(), 2)
        self.assertEqual(self.peers['alice'][0].sent[-1],
                         'ROOM_PEER_MSG new hello λ')

    async def test_closed_recipient_does_not_disconnect_joiner_or_skip_others(self):
        for exception in (ConnectionClosedOK, ConnectionClosedError):
            with self.subTest(exception=exception.__name__):
                self.room('alice', 'bob', 'carol')
                closed_ids = []
                def closer(uid):
                    async def closed(message):
                        if not closed_ids:
                            closed_ids.append(uid)
                            raise exception(None, None)
                    return closed
                for uid in self.rooms['room']:
                    self.peers[uid][0].on_send = closer(uid)
                ws, task = self.joining()
                await self.ready(ws, task)
                self.assertEqual(len(closed_ids), 1)
                for uid in {'alice', 'bob', 'carol'} - set(closed_ids):
                    self.assertEqual(self.peers[uid][0].sent,
                                     ['ROOM_PEER_JOINED new'])
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def mutate_while_sending(self, change):
        self.room('alice', 'bob', 'carol', 'dave')
        sent_ids = []
        entered, release = asyncio.Event(), asyncio.Event()
        def blocker(uid):
            async def blocked(message):
                sent_ids.append(uid)
                if len(sent_ids) == 1:
                    entered.set()
                    await release.wait()
            return blocked
        for uid in self.rooms['room']:
            self.peers[uid][0].on_send = blocker(uid)
        ws, task = self.joining()
        try:
            await asyncio.wait_for(entered.wait(), 2)
            order = [uid for uid in self.rooms['room']
                     if uid not in ('new', sent_ids[0])]
            later = order[0]
            result = change(later)
        finally:
            release.set()
        await self.ready(ws, task)
        for uid in order[1:]:
            self.assertEqual(self.peers[uid][0].sent, ['ROOM_PEER_JOINED new'])
        return result

    async def test_departure_while_sending_does_not_abort_join(self):
        def leave(uid):
            self.rooms['room'].remove(uid)
            return self.peers.pop(uid)[0]
        removed = await self.mutate_while_sending(leave)
        self.assertEqual(removed.sent, [])

    async def test_later_arrival_is_not_notified_twice(self):
        def arrive(uid):
            # A subsequent join sees 'new' in its own ROOM_OK roster.
            ws = Peer()
            self.peers['later'] = [ws, ('test', 1), 'room']
            self.rooms['room'].add('later')
            return ws
        arrived = await self.mutate_while_sending(arrive)
        self.assertEqual(arrived.sent, [])

    async def test_reconnected_peer_in_another_room_is_skipped(self):
        def move(uid):
            self.rooms['room'].remove(uid)
            self.rooms['elsewhere'] = {uid}
            replacement = Peer()
            self.peers[uid] = [replacement, ('test', 1), 'elsewhere']
            return replacement
        replacement = await self.mutate_while_sending(move)
        self.assertEqual(replacement.sent, [])

    async def test_unexpected_notification_error_propagates(self):
        self.room('alice')
        async def broken(message):
            raise ValueError('unexpected failure')
        self.peers['alice'][0].on_send = broken
        _, task = self.joining()
        with self.assertRaisesRegex(ValueError, 'unexpected failure'):
            await asyncio.wait_for(task, 2)

    async def test_failed_initial_roster_does_not_insert_or_notify(self):
        self.room('alice')
        async def broken(message):
            raise ConnectionClosedError(None, None)
        _, task = self.joining(broken)
        with self.assertRaises(ConnectionClosedError):
            await asyncio.wait_for(task, 2)
        self.assertIsNone(self.peers['new'][2])
        self.assertEqual(self.rooms['room'], {'alice'})
        self.assertEqual(self.peers['alice'][0].sent, [])


class ErrorLog(logging.Handler):
    def __init__(self):
        super().__init__(logging.ERROR)
        self.records = []

    def emit(self, record):
        self.records.append(record)


class RoomJoinTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.namespace = load_server()
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.blocked = None
        self.clients = {}
        test = self

        class GatedProtocol(WebSocketServerProtocol):
            async def send(self, message):
                if message == 'ROOM_PEER_JOINED new' and test.blocked is None:
                    test.blocked = self
                    test.entered.set()
                    await test.release.wait()
                return await super().send(message)

        self.errors = ErrorLog()
        logger = logging.getLogger(self.id())
        logger.addHandler(self.errors)
        self.addCleanup(logger.removeHandler, self.errors)
        self.server = await serve(self.namespace['handler'], '127.0.0.1', 0,
                                  create_protocol=GatedProtocol, logger=logger)
        self.uri = 'ws://127.0.0.1:{}'.format(
            self.server.sockets[0].getsockname()[1])
        self.addAsyncCleanup(self.shutdown)

    async def shutdown(self):
        self.release.set()
        for uid, ws in self.clients.items():
            await ws.close()
            await self.removed(uid)
        self.server.close()
        await asyncio.wait_for(self.server.wait_closed(), 3)
        self.assertEqual(self.namespace['peers'], {})
        self.assertTrue(all(not members for members in self.namespace['rooms'].values()))
        self.assertEqual([r.getMessage() for r in self.errors.records], [])

    async def removed(self, uid):
        async def wait():
            while uid in self.namespace['peers']:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), 2)

    async def message(self, ws):
        return await asyncio.wait_for(ws.recv(), 2)

    async def connect(self, uid):
        ws = await websockets.connect(self.uri, open_timeout=2, close_timeout=1)
        self.clients[uid] = ws
        await ws.send('HELLO ' + uid)
        self.assertEqual(await self.message(ws), 'HELLO')
        return ws

    async def join_existing(self, uid):
        previous = list(self.clients.values())
        ws = await self.connect(uid)
        await ws.send('ROOM room')
        self.assertTrue((await self.message(ws)).startswith('ROOM_OK '))
        for other in previous:
            self.assertEqual(await self.message(other), 'ROOM_PEER_JOINED ' + uid)

    async def interrupted_join(self, close_blocked):
        for uid in ('alice', 'bob', 'carol'):
            await self.join_existing(uid)
        new = await self.connect('new')
        await new.send('ROOM room')
        self.assertTrue((await self.message(new)).startswith('ROOM_OK '))
        await asyncio.wait_for(self.entered.wait(), 2)
        blocked_id = next(uid for uid, peer in self.namespace['peers'].items()
                          if peer[0] is self.blocked)
        victim = blocked_id if close_blocked else next(
            uid for uid in ('alice', 'bob', 'carol') if uid != blocked_id)
        await self.clients[victim].close()
        await self.removed(victim)
        self.release.set()
        await new.send('invalid command')
        self.assertEqual(await self.message(new), 'ROOM_PEER_LEFT ' + victim)
        self.assertEqual(await self.message(new), 'ERROR invalid msg, already in room')
        survivors = set(('alice', 'bob', 'carol')) - {victim}
        for uid in survivors:
            notices = [await self.message(self.clients[uid]) for _ in range(2)]
            self.assertCountEqual(notices, ['ROOM_PEER_LEFT ' + victim,
                                           'ROOM_PEER_JOINED new'])
        survivor = next(iter(survivors))
        await new.send('ROOM_PEER_MSG {} hello λ'.format(survivor))
        self.assertEqual(await self.message(self.clients[survivor]),
                         'ROOM_PEER_MSG new hello λ')
        await self.clients[survivor].send('ROOM_PEER_MSG new still here')
        self.assertEqual(await self.message(new),
                         'ROOM_PEER_MSG {} still here'.format(survivor))

    async def test_closed_notification_transport_keeps_joiner_connected(self):
        await self.interrupted_join(close_blocked=True)

    async def test_other_departure_during_notification_keeps_joiner_connected(self):
        await self.interrupted_join(close_blocked=False)


if __name__ == '__main__':
    unittest.main()
