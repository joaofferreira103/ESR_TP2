"""Testes da gestão de vizinhos, com vários nós em 127.0.0.x na mesma máquina."""

import logging
import socket
import threading
import time
import unittest

from ott import config
from ott.neighbors import NeighborManager
from ott.protocol import control
from ott.state import NodeState
from ott.transport.tcp import TcpTransport

WAIT_TIMEOUT = 3.0


MAX_TEST_NODES = 10


def free_port():
    """Porta TCP livre em todos os IPs dos nós de teste (cada nó tem o seu IP 127.0.0.x, mesma porta).

    Não basta verificar 127.0.0.1: a porta pode estar ocupada noutro 127.0.0.x por uma
    ligação de saída de um teste anterior.
    """
    while True:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        try:
            for index in range(1, MAX_TEST_NODES + 1):
                with socket.socket() as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind((f"127.0.0.{20 + index}", port))
            return port
        except OSError:
            continue


class ListHandler(logging.Handler):
    """Guarda as mensagens de log numa lista, para os testes as verificarem."""

    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(f"{record.levelname} {record.getMessage()}")


def logged(manager, text):
    return any(text in line for line in manager.log_lines)


def wait_until(condition, timeout=WAIT_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


class NeighborManagerTest(unittest.TestCase):
    def setUp(self):
        self.port = free_port()
        self.managers = []

    def tearDown(self):
        for m in self.managers:
            m.stop()

    def make_node(self, index):
        """Cria e arranca o nó O<index> em 127.0.0.<20+index>. As linhas de log ficam em manager.log_lines.

        Os eventos são registados por outras threads, e às vezes DEPOIS de o estado mudar
        (ex.: _drop remove o vizinho e só depois escreve no log): os testes esperam pela
        linha de log com wait_until, em vez de a verificarem logo.
        """
        node_id = f"O{index}"
        logger = logging.getLogger(f"ott.test.{self.id()}.{node_id}")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        capture = ListHandler()
        logger.addHandler(capture)
        factory = lambda on_message, on_closed: TcpTransport(on_message, on_closed, logger, self.ip(index))
        manager = NeighborManager(NodeState(node_id), logger, factory, port=self.port)
        manager.log_lines = capture.lines
        manager.start()
        self.managers.append(manager)
        return manager

    @staticmethod
    def ip(index):
        return f"127.0.0.{20 + index}"

    def test_two_nodes_connect(self):
        a, b = self.make_node(1), self.make_node(2)
        self.assertTrue(a.connect_to("O2", self.ip(2)))
        self.assertTrue(wait_until(lambda: b.state.count_up() == 1))
        self.assertEqual(a.state.get_neighbor("O2").status, "up")
        self.assertTrue(a.state.get_neighbor("O2").initiated_by_me)
        self.assertFalse(b.state.get_neighbor("O1").initiated_by_me)
        self.assertEqual(b.state.get_neighbor("O1").ip, self.ip(1))

    def test_simultaneous_connect_keeps_lower_id(self):
        a, b = self.make_node(1), self.make_node(2)
        downs = []
        a.add_down_listener(lambda nid, reason: downs.append(nid))
        b.add_down_listener(lambda nid, reason: downs.append(nid))
        # Reservar os dois lados antes de qualquer HELLO garante que as tentativas se cruzam
        na = a._reserve("O2", self.ip(2))
        nb = b._reserve("O1", self.ip(1))
        threads = [threading.Thread(target=a._handshake, args=(na,)),
                   threading.Thread(target=b._handshake, args=(nb,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join(WAIT_TIMEOUT)

        self.assertTrue(wait_until(lambda: a.state.count_up() == 1 and b.state.count_up() == 1))
        time.sleep(0.2)  # a ligação abandonada fecha-se: a que ficou tem de continuar "up"
        self.assertEqual(a.state.count_up(), 1)
        self.assertEqual(b.state.count_up(), 1)
        # Ficou a ligação iniciada por O1 (ID menor), e os dois lados concordam
        self.assertTrue(a.state.get_neighbor("O2").initiated_by_me)
        self.assertFalse(b.state.get_neighbor("O1").initiated_by_me)
        self.assertEqual(downs, [])
        # A ligação funciona: uma mensagem de O2 chega a O1
        received = threading.Event()
        a.register_handler("TESTE", lambda nid, msg: received.set())
        self.assertTrue(b.send_to("O1", control.make_msg("TESTE", "O2")))
        self.assertTrue(received.wait(WAIT_TIMEOUT))

    def test_full_rejects(self):
        hub = self.make_node(1)
        for i in range(2, 2 + config.X_MAX):
            self.assertTrue(self.make_node(i).connect_to("O1", self.ip(1)))
        self.assertTrue(wait_until(lambda: hub.state.count_up() == config.X_MAX))

        extra = self.make_node(2 + config.X_MAX)
        self.assertFalse(extra.connect_to("O1", self.ip(1)))
        self.assertTrue(logged(extra, "rejeitada: full"))
        self.assertTrue(wait_until(lambda: logged(hub, f"HELLO de {extra.state.node_id} rejeitado (full)")))
        self.assertEqual(hub.state.count_up(), config.X_MAX)
        self.assertIsNone(extra.state.get_neighbor("O1"))

    def test_full_does_not_open_outgoing_connections(self):
        hub = self.make_node(1)
        for i in range(2, 2 + config.X_MAX):
            self.assertTrue(hub.connect_to(self.make_node(i).state.node_id, self.ip(i)))
        extra = self.make_node(2 + config.X_MAX)
        self.assertFalse(hub.connect_to(extra.state.node_id, self.ip(2 + config.X_MAX)))
        self.assertEqual(hub.state.count_up(), config.X_MAX)

    def test_accept_survives_temporary_error(self):
        a, b = self.make_node(1), self.make_node(2)
        real_sock = b._listener._sock

        class FailsOnce:
            def __init__(self):
                self.failed = False

            def accept(self):
                if not self.failed:
                    self.failed = True
                    raise OSError("sem descritores livres")
                return real_sock.accept()

            def __getattr__(self, name):
                return getattr(real_sock, name)

        # Acordar a thread, bloqueada no accept() anterior, com uma ligação que falha o HELLO
        b._listener._sock = FailsOnce()
        with socket.create_connection((self.ip(2), self.port), timeout=WAIT_TIMEOUT):
            pass
        self.assertTrue(wait_until(lambda: b._listener._sock.failed or logged(b, "sem HELLO")))
        self.assertTrue(a.connect_to("O2", self.ip(2)))
        self.assertTrue(wait_until(lambda: logged(b, "sem descritores livres")))

    def test_leave_removes_neighbor(self):
        a, b = self.make_node(1), self.make_node(2)
        reasons = []
        b.add_down_listener(lambda nid, reason: reasons.append((nid, reason)))
        self.assertTrue(a.connect_to("O2", self.ip(2)))
        self.assertTrue(wait_until(lambda: b.state.count_up() == 1))

        a.stop()
        self.assertTrue(wait_until(lambda: b.state.count_up() == 0))
        self.assertTrue(wait_until(lambda: logged(b, "O1 saiu")), b.log_lines)
        self.assertTrue(wait_until(lambda: reasons == [("O1", "saiu")]), reasons)

    def test_crash_detected_once(self):
        a, b = self.make_node(1), self.make_node(2)
        downs = []
        a.add_down_listener(lambda nid, reason: downs.append(nid))
        self.assertTrue(a.connect_to("O2", self.ip(2)))
        self.assertTrue(wait_until(lambda: b.state.count_up() == 1))

        # "Matar" O2: fechar os sockets sem LEAVE, como num crash
        b._listener.close()
        for n in list(b.state.neighbors.values()):
            n.channel.close()
        # Enviar ao mesmo tempo, para a leitura e a escrita de O1 falharem quase em simultâneo
        for _ in range(100):
            a.send_to("O2", control.make_msg("TESTE", "O1"))

        self.assertTrue(wait_until(lambda: a.state.count_up() == 0))
        time.sleep(0.2)
        self.assertEqual(downs, ["O2"])

    def test_unknown_message_type_does_not_close(self):
        a, b = self.make_node(1), self.make_node(2)
        self.assertTrue(a.connect_to("O2", self.ip(2)))
        self.assertTrue(wait_until(lambda: b.state.count_up() == 1))
        a.send_to("O2", control.make_msg("DESCONHECIDO", "O1"))
        self.assertTrue(wait_until(lambda: logged(b, "DESCONHECIDO de O1 sem handler")))
        self.assertEqual(b.state.count_up(), 1)


if __name__ == "__main__":
    unittest.main()
