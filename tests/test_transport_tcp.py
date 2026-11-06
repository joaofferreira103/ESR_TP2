"""Testes do canal TCP: fila de envio e thread escritora (antes em state.Neighbor)."""

import socket
import threading
import time
import unittest

from ott import config
from ott.protocol import control
from ott.transport.tcp import TcpChannel


def ignore(*_):
    pass


class TcpChannelTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = socket.socketpair()
        self.b.settimeout(5)

    def tearDown(self):
        self.a.close()
        self.b.close()

    def make_channel(self, on_closed=ignore, on_message=ignore):
        return TcpChannel("O2", "10.0.0.2", self.a, on_message, on_closed)

    def test_send_full_queue_does_not_block(self):
        n = self.make_channel()  # sem start(): a fila não esvazia
        for i in range(config.SEND_QUEUE_MAX):
            self.assertTrue(n.send({"i": i}))
        start = time.monotonic()
        self.assertFalse(n.send({"i": "a mais"}))
        self.assertLess(time.monotonic() - start, 0.1)

    def test_writer_sends_in_order(self):
        n = self.make_channel()
        n.start()
        messages = [control.make_msg(control.HELLO, "O1", n=i) for i in range(20)]
        for msg in messages:
            self.assertTrue(n.send(msg))
        for msg in messages:
            self.assertEqual(control.recv_msg(self.b), msg)
        n.close()

    def test_on_closed_once_when_peer_closes(self):
        closed = []
        called = threading.Event()

        def on_closed(nid, reason):
            closed.append(nid)
            called.set()

        n = self.make_channel(on_closed)
        n.start()
        self.b.close()
        n.send(control.make_msg(control.HELLO, "O1"))
        self.assertTrue(called.wait(2))
        n._writer.join(2)
        self.assertFalse(n._writer.is_alive())
        self.assertEqual(closed, ["O2"])
        self.assertTrue(n.closed)

    def test_close_stops_writer_without_on_closed(self):
        closed = []
        n = self.make_channel(lambda nid, reason: closed.append(nid))
        n.start()
        n.close()
        n._writer.join(2)
        self.assertFalse(n._writer.is_alive())
        self.assertEqual(closed, [])

    def test_on_message_delivers_received(self):
        received = threading.Event()
        got = []

        def on_message(nid, msg):
            got.append((nid, msg["type"]))
            received.set()

        n = self.make_channel(on_message=on_message)
        n.start()
        control.send_msg(self.b, control.make_msg("TESTE", "O2"))
        self.assertTrue(received.wait(2))
        self.assertEqual(got, [("O2", "TESTE")])
        n.close()


if __name__ == "__main__":
    unittest.main()
