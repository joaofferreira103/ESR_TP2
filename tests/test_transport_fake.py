"""Prova que o NeighborManager não depende de TCP: corre sobre um transporte falso, em memória."""

import ast
import logging
import os
import queue
import threading
import time
import unittest

from ott import config
from ott.neighbors import NeighborManager
from ott.node import create_transport
from ott.protocol import control
from ott.state import NodeState
from ott.transport.base import ControlChannel, ControlListener, ControlTransport, TransportError
from ott.transport.tcp import TcpTransport

WAIT_TIMEOUT = 3.0
_STOP = object()
# Raiz do projeto, calculada a partir deste ficheiro: os testes passam seja qual for a pasta de onde se corram
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def wait_until(condition, timeout=WAIT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


class FakeChannel(ControlChannel):
    """Metade de uma ligação em memória: o que se envia entra na caixa de entrada do outro lado."""

    def __init__(self, neighbor_id, peer_ip, on_message, on_closed):
        super().__init__(neighbor_id, peer_ip, on_message, on_closed)
        self.peer = None
        self.handshake_replies = None   # onde o lado que recebeu o HELLO põe a resposta
        self.inbox = queue.Queue()
        self.blocked = False            # True simula um vizinho que deixou de escoar a fila
        self._closed = False
        self._lock = threading.Lock()

    def start(self):
        threading.Thread(target=self._deliver_loop, daemon=True).start()

    def respond(self, msg):
        self.handshake_replies.put(msg)

    def send(self, msg):
        if self.blocked:
            return False
        self.peer.inbox.put(msg)
        return True

    def flush(self, timeout):
        return not self.closed

    def close(self):
        if self._mark_closed():
            self.inbox.put(_STOP)
            self.peer.peer_closed()

    @property
    def closed(self):
        with self._lock:
            return self._closed

    def pending(self):
        return 0

    def peer_closed(self):
        """O outro lado fechou: para este lado é uma falha, avisada com on_closed."""
        self.inbox.put(("closed", "ligação fechada"))

    def _mark_closed(self):
        with self._lock:
            if self._closed:
                return False
            self._closed = True
            return True

    def _deliver_loop(self):
        while True:
            item = self.inbox.get()
            if item is _STOP:
                return
            if isinstance(item, tuple):
                if self._mark_closed():
                    self._on_closed(self.neighbor_id, item[1])
                return
            if self.closed:
                return
            self._on_message(self.neighbor_id, item)


class FakeListener(ControlListener):
    def __init__(self, network, address, on_channel, on_message, on_closed):
        self.network = network
        self.address = address
        self.on_channel = on_channel
        self.on_message = on_message
        self.on_closed = on_closed

    def start(self):
        self.network[self.address] = self

    def close(self):
        self.network.pop(self.address, None)


class FakeTransport(ControlTransport):
    def __init__(self, network, ip, on_message, on_closed):
        super().__init__(on_message, on_closed)
        self.network = network
        self.ip = ip

    def listen(self, port, on_channel):
        return FakeListener(self.network, (self.ip, port), on_channel, self._on_message, self._on_closed)

    def connect(self, ip, port, hello_msg, timeout):
        listener = self.network.get((ip, port))
        if listener is None:
            raise TransportError("ninguém à escuta")
        replies = queue.Queue()
        remote = FakeChannel(hello_msg["src"], self.ip, listener.on_message, listener.on_closed)
        remote.handshake_replies = replies
        # O id do lado local só se sabe com a resposta; o par fica ligado antes de o outro lado poder enviar
        local = FakeChannel(None, ip, self._on_message, self._on_closed)
        local.peer, remote.peer = remote, local
        threading.Thread(target=listener.on_channel, args=(remote, hello_msg), daemon=True).start()
        try:
            reply = replies.get(timeout=timeout)
        except queue.Empty:
            raise TransportError("sem resposta ao HELLO") from None
        local.neighbor_id = reply["src"]
        return local, reply


class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


class NeighborManagerOverFakeTransportTest(unittest.TestCase):
    def setUp(self):
        self.network = {}
        self.managers = []

    def tearDown(self):
        for manager in self.managers:
            manager.stop()

    def make_node(self, index):
        node_id = f"O{index}"
        logger = logging.getLogger(f"ott.test.{self.id()}.{node_id}")
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        capture = ListHandler()
        logger.addHandler(capture)
        ip = f"fake-{index}"
        factory = lambda on_message, on_closed: FakeTransport(self.network, ip, on_message, on_closed)
        manager = NeighborManager(NodeState(node_id), logger, factory)
        manager.log_lines = capture.lines
        manager.start()
        self.managers.append(manager)
        return manager

    def connected_pair(self):
        a, b = self.make_node(1), self.make_node(2)
        self.assertTrue(a.connect_to("O2", "fake-2"))
        self.assertTrue(wait_until(lambda: b.state.count_up() == 1))
        return a, b

    def test_connect_and_exchange_messages(self):
        a, b = self.connected_pair()
        self.assertEqual(a.state.get_neighbor("O2").status, "up")
        self.assertEqual(b.state.get_neighbor("O1").ip, "fake-1")
        self.assertIsInstance(a.state.get_neighbor("O2").channel, FakeChannel)

        received = []
        arrived = threading.Event()

        def handler(neighbor_id, msg):
            received.append((neighbor_id, msg["n"]))
            arrived.set()

        b.register_handler("TESTE", handler)
        self.assertTrue(a.send_to("O2", control.make_msg("TESTE", "O1", n=7)))
        self.assertTrue(arrived.wait(WAIT_TIMEOUT))
        self.assertEqual(received, [("O1", 7)])

    def test_connect_failure_removes_reservation(self):
        a = self.make_node(1)
        self.assertFalse(a.connect_to("O9", "fake-9"))
        self.assertIsNone(a.state.get_neighbor("O9"))

    def test_full_rejects(self):
        hub = self.make_node(1)
        for i in range(2, 2 + config.X_MAX):
            self.assertTrue(self.make_node(i).connect_to("O1", "fake-1"))
        extra = self.make_node(2 + config.X_MAX)
        self.assertFalse(extra.connect_to("O1", "fake-1"))
        self.assertTrue(any("rejeitada: full" in line for line in extra.log_lines))
        self.assertEqual(hub.state.count_up(), config.X_MAX)

    def test_leave_removes_neighbor(self):
        a, b = self.connected_pair()
        reasons = []
        b.add_down_listener(lambda nid, reason: reasons.append((nid, reason)))
        a.stop()
        self.assertTrue(wait_until(lambda: reasons == [("O1", "saiu")]), reasons)
        self.assertEqual(b.state.count_up(), 0)

    def test_crash_detected_once(self):
        a, b = self.connected_pair()
        downs = []
        a.add_down_listener(lambda nid, reason: downs.append((nid, reason)))
        b.state.get_neighbor("O1").channel.close()  # sem LEAVE, como num crash
        self.assertTrue(wait_until(lambda: a.state.count_up() == 0))
        self.assertTrue(wait_until(lambda: downs == [("O2", "ligação fechada")]), downs)

    def test_send_failure_drops_neighbor(self):
        a, _ = self.connected_pair()
        downs = []
        a.add_down_listener(lambda nid, reason: downs.append((nid, reason)))
        a.state.get_neighbor("O2").channel.blocked = True
        self.assertFalse(a.send_to("O2", control.make_msg("TESTE", "O1")))
        self.assertEqual(a.state.count_up(), 0)
        self.assertEqual(downs, [("O2", "fila de envio cheia")])

    def test_stale_on_closed_does_not_drop_new_connection(self):
        """Um aviso atrasado de uma ligação antiga não pode derrubar a ligação nova com o mesmo id."""
        a, _ = self.connected_pair()
        a._on_channel_closed("O2", "ligação fechada")  # o canal atual está aberto: o aviso é antigo
        self.assertEqual(a.state.count_up(), 1)


class TransportIsolationTest(unittest.TestCase):
    def imported_modules(self, path):
        with open(os.path.join(PROJECT_ROOT, path), encoding="utf-8") as f:
            tree = ast.parse(f.read())
        names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                names.add(node.module)
        return names

    def test_no_socket_outside_transport(self):
        for path in ("ott/neighbors.py", "ott/node.py", "ott/state.py", "ott/repair.py"):
            self.assertNotIn("socket", self.imported_modules(path), path)

    def test_only_node_factory_imports_tcp(self):
        for path in ("ott/neighbors.py", "ott/state.py", "ott/repair.py"):
            self.assertNotIn("ott.transport.tcp", self.imported_modules(path), path)

    def test_factory_chooses_implementation(self):
        self.assertEqual(config.CONTROL_TRANSPORT, "tcp")
        transport = create_transport("tcp", "127.0.0.1", None, None, logging.getLogger("ott.test"))
        self.assertIsInstance(transport, TcpTransport)
        with self.assertRaises(ValueError):
            create_transport("pombo-correio", "127.0.0.1", None, None, logging.getLogger("ott.test"))


if __name__ == "__main__":
    unittest.main()
