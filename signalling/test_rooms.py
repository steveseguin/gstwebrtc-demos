"""Real WebSocket room regressions; see ROOM_TESTS.md for validation limits."""
import ast
import asyncio
from contextlib import AsyncExitStack
from pathlib import Path
import sys
import unittest
from unittest import mock

import websockets
from websockets.legacy.server import serve


SERVER = Path(__file__).with_name('simple-server.py')


def load_server():
    source = ast.parse(SERVER.read_text(), filename=str(SERVER))
    # Keep the real imports, argument parsing, globals, and handlers. Only the
    # independent command-line listener and certificate setup are excluded.
    boundary = next(i for i, node in enumerate(source.body) if isinstance(
        node, ast.Assign) and any(isinstance(target, ast.Name) and
        target.id == 'certpath' for target in node.targets))
    source.body = source.body[:boundary]
    namespace = {'__file__': str(SERVER), '__name__': 'room_test_server'}
    with mock.patch.object(sys, 'argv', [str(SERVER)]):
        exec(compile(source, str(SERVER), 'exec'), namespace)
    return namespace


class RoomTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.namespace = load_server()
        # Explicit fixture API avoids coupling these handler regressions to
        # the separate standalone startup compatibility and TLS fixes.
        self.server = await serve(self.namespace['handler'], '127.0.0.1', 0,
                                  max_queue=16)
        self.uri = 'ws://127.0.0.1:{}'.format(
            self.server.sockets[0].getsockname()[1])
        self.clients = AsyncExitStack()
        self.opened = []

    async def asyncTearDown(self):
        # Serial teardown isolates these room-list tests from the existing
        # rapid-disconnect cleanup race. A client close handshake alone does
        # not guarantee that its server handler has finished removing it.
        for uid, ws in reversed(self.opened):
            await ws.close()
            async def removed():
                while (uid in self.namespace['peers'] and
                       self.namespace['peers'][uid][0].remote_address ==
                       ws.local_address):
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(removed(), 2)
        await self.clients.aclose()
        self.server.close()
        await asyncio.wait_for(self.server.wait_closed(), 3)
        self.assertEqual(self.namespace['peers'], {})
        self.assertEqual(self.namespace['sessions'], {})
        self.assertTrue(all(not members for members in self.namespace['rooms'].values()))

    async def message(self, ws):
        return await asyncio.wait_for(ws.recv(), 2)

    async def peer(self, uid):
        ws = await self.clients.enter_async_context(websockets.connect(
            self.uri, open_timeout=2, close_timeout=1))
        self.opened.append((uid, ws))
        await ws.send('HELLO ' + uid)
        self.assertEqual(await self.message(ws), 'HELLO')
        return ws

    async def join(self, ws, room, expected):
        await ws.send('ROOM ' + room)
        message = await self.message(ws)
        self.assertTrue(message.startswith('ROOM_OK '), message)
        self.assertEqual(set(message[len('ROOM_OK '):].split()), set(expected))

    async def members(self, ws, expected):
        await ws.send('ROOM_PEER_LIST')
        message = await self.message(ws)
        self.assertTrue(message.startswith('ROOM_PEER_LIST '), message)
        listed = message[len('ROOM_PEER_LIST '):].split()
        self.assertEqual(len(listed), len(set(listed)))
        self.assertEqual(set(listed), set(expected))

    async def test_single_member_can_list_repeatedly(self):
        alice = await self.peer('alice')
        await self.join(alice, 'conference', [])
        for _ in range(3):
            await self.members(alice, [])

    async def test_each_list_excludes_self_and_other_rooms(self):
        alice, bob, carol, outsider = [await self.peer(uid) for uid in
                                        ('alice', 'bob', 'carol', 'outsider')]
        await self.join(alice, 'conference', [])
        await self.join(bob, 'conference', ['alice'])
        self.assertEqual(await self.message(alice), 'ROOM_PEER_JOINED bob')
        await self.join(carol, 'conference', ['alice', 'bob'])
        self.assertEqual(await self.message(alice), 'ROOM_PEER_JOINED carol')
        self.assertEqual(await self.message(bob), 'ROOM_PEER_JOINED carol')
        await self.join(outsider, 'elsewhere', [])
        await self.members(alice, ['bob', 'carol'])
        await self.members(bob, ['alice', 'carol'])
        await self.members(carol, ['alice', 'bob'])
        await self.members(outsider, [])

    async def test_lists_follow_leave_rejoin_and_keep_relay_usable(self):
        alice, bob = [await self.peer(uid) for uid in ('alice', 'bob')]
        await self.join(alice, 'conference', [])
        await self.join(bob, 'conference', ['alice'])
        self.assertEqual(await self.message(alice), 'ROOM_PEER_JOINED bob')
        await self.members(alice, ['bob'])
        await bob.close()
        self.assertEqual(await self.message(alice), 'ROOM_PEER_LEFT bob')
        await self.members(alice, [])
        bob = await self.peer('bob')
        await self.join(bob, 'conference', ['alice'])
        self.assertEqual(await self.message(alice), 'ROOM_PEER_JOINED bob')
        await self.members(alice, ['bob'])
        await self.members(bob, ['alice'])
        for sender, receiver, uid, target, payload in [
            (alice, bob, 'alice', 'bob', '{"sdp":"synthetic offer λ"}'),
            (bob, alice, 'bob', 'alice', '{"ice":"synthetic reply"}'),
        ]:
            await sender.send('ROOM_PEER_MSG {} {}'.format(target, payload))
            self.assertEqual(await self.message(receiver),
                             'ROOM_PEER_MSG {} {}'.format(uid, payload))

    async def test_relay_and_recipient_errors_without_listing(self):
        alice, bob, outsider = [await self.peer(uid) for uid in
                               ('alice', 'bob', 'outsider')]
        await self.join(alice, 'conference', [])
        await self.join(bob, 'conference', ['alice'])
        self.assertEqual(await self.message(alice), 'ROOM_PEER_JOINED bob')
        await self.join(outsider, 'elsewhere', [])
        await alice.send('ROOM_PEER_MSG bob hello λ')
        self.assertEqual(await self.message(bob), 'ROOM_PEER_MSG alice hello λ')
        for target, error in [('missing', "ERROR peer 'missing' not found"),
                              ('outsider', "ERROR peer 'outsider' is not in the room")]:
            await alice.send('ROOM_PEER_MSG {} ignored'.format(target))
            self.assertEqual(await self.message(alice), error)
        await alice.send('ROOM_PEER_MSG bob still connected')
        self.assertEqual(await self.message(bob), 'ROOM_PEER_MSG alice still connected')

    async def test_leave_and_reconnect_without_listing(self):
        alice, bob = [await self.peer(uid) for uid in ('alice', 'bob')]
        await self.join(alice, 'conference', [])
        await self.join(bob, 'conference', ['alice'])
        self.assertEqual(await self.message(alice), 'ROOM_PEER_JOINED bob')
        await bob.close()
        self.assertEqual(await self.message(alice), 'ROOM_PEER_LEFT bob')
        bob = await self.peer('bob')
        await self.join(bob, 'conference', ['alice'])
        self.assertEqual(await self.message(alice), 'ROOM_PEER_JOINED bob')


if __name__ == '__main__':
    unittest.main()
