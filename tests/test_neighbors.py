"""Testes da gestão de vizinhos, com vários nós em 127.0.0.x na mesma máquina."""

import logging
import socket
import threading
import time
import unittest
from unittest import mock

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

    def raw_hello(self, node_index, src):
        """Ligação TCP em bruto que envia HELLO como src e lê a resposta, sem enviar HELLO_CONFIRM."""
        sock = socket.create_connection((self.ip(node_index), self.port), timeout=WAIT_TIMEOUT)
        control.send_msg(sock, control.make_msg(control.HELLO, src))
        return sock, control.recv_msg(sock)

    def watch_events(self, manager):
        events = []
        manager.add_up_listener(lambda nid: events.append(("up", nid)))
        manager.add_down_listener(lambda nid, reason: events.append(("down", nid)))
        return events

    def test_sender_closes_before_confirm_generates_no_event(self):
        """Erro 2: uma ligação abandonada antes do HELLO_CONFIRM não gera up nem down."""
        a = self.make_node(1)
        events = self.watch_events(a)
        sock, reply = self.raw_hello(1, "O9")
        self.assertEqual(reply["type"], control.HELLO_ACK)
        sock.close()
        self.assertTrue(wait_until(lambda: logged(a, "descartada antes do HELLO_CONFIRM")))
        time.sleep(0.1)
        self.assertEqual(events, [])
        self.assertIsNone(a.state.get_neighbor("O9"))
        self.assertFalse(any(line.startswith(("WARNING", "ERROR")) for line in a.log_lines), a.log_lines)

    def test_missing_confirm_expires_silently(self):
        a = self.make_node(1)
        events = self.watch_events(a)
        with mock.patch.object(config, "HELLO_TIMEOUT", 0.3):
            sock, _ = self.raw_hello(1, "O9")
            self.assertTrue(wait_until(lambda: logged(a, "descartada antes do HELLO_CONFIRM")))
        sock.close()
        self.assertEqual(events, [])
        self.assertIsNone(a.state.get_neighbor("O9"))

    def test_neighbor_is_not_up_before_confirm(self):
        a = self.make_node(1)
        sock, _ = self.raw_hello(1, "O9")
        self.assertTrue(wait_until(lambda: a.state.get_neighbor("O9") is not None))
        self.assertEqual(a.state.get_neighbor("O9").status, "connecting")
        self.assertEqual(a.state.count_up(), 0)
        control.send_msg(sock, control.make_msg(control.HELLO_CONFIRM, "O9"))
        self.assertTrue(wait_until(lambda: a.state.count_up() == 1))
        sock.close()

    def test_pending_connections_count_for_x_max(self):
        """HELLOs à espera do HELLO_CONFIRM ocupam vaga: o seguinte recebe "full"."""
        self.make_node(1)
        pending = [self.raw_hello(1, f"O{90 + i}") for i in range(config.X_MAX)]
        self.assertTrue(all(reply["type"] == control.HELLO_ACK for _, reply in pending))
        sock, reply = self.raw_hello(1, "O99")
        self.assertEqual((reply["type"], reply.get("reason")), (control.HELLO_REJECT, "full"))
        for s, _ in pending + [(sock, None)]:
            s.close()

    def test_new_hello_replaces_stale_pending_one(self):
        """Uma ligação velha da fila (sem CONFIRM) não impede a ligação nova do mesmo vizinho."""
        a, b = self.make_node(1), self.make_node(2)
        events = self.watch_events(a)
        stale, _ = self.raw_hello(1, "O2")  # fica aberta, sem HELLO_CONFIRM
        self.assertTrue(wait_until(lambda: a.state.get_neighbor("O2") is not None))
        self.assertTrue(b.connect_to("O1", self.ip(1)))
        self.assertTrue(wait_until(lambda: a.state.count_up() == 1))
        stale.close()
        time.sleep(0.2)
        self.assertEqual(events, [("up", "O2")])
        self.assertEqual(a.state.get_neighbor("O2").status, "up")

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


class ReconnectOnDownTest(unittest.TestCase):
    """Erro 8: quando um vizinho sai, tenta-se logo ligar aos vizinhos estáticos em falta (sem rede)."""

    def make_manager(self, static, connected):
        logger = logging.getLogger(f"ott.test.{self.id()}")
        logger.propagate = False
        state = NodeState("O4")
        state.static_neighbors = static
        manager = NeighborManager(state, logger, lambda on_message, on_closed: None)
        for neighbor_id in connected:
            neighbor = mock.Mock(status="up")
            state.neighbors[neighbor_id] = neighbor
        manager.connect_async = mock.Mock()
        return manager

    def test_down_triggers_reconnect_to_missing_static(self):
        # R6: O4 (cheio com O2, O5, O8 e O7 dinâmico) perde O7; O6, estático, está em falta
        m = self.make_manager({"O2": "ip2", "O5": "ip5", "O6": "ip6", "O8": "ip8"}, ["O2", "O5", "O8"])
        m.on_neighbor_down("O7", "saiu")
        m.connect_async.assert_called_once_with("O6", "ip6")

    def test_neighbor_that_just_went_down_is_not_retried_at_once(self):
        m = self.make_manager({"O2": "ip2", "O6": "ip6"}, ["O2"])
        m.on_neighbor_down("O6", "ligação fechada")
        m.connect_async.assert_not_called()

    def test_no_reconnect_while_stopping(self):
        m = self.make_manager({"O2": "ip2", "O6": "ip6"}, ["O2"])
        m._stopping = True
        m.on_neighbor_down("O7", "saiu")
        m.connect_async.assert_not_called()

