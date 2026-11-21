"""Testes da autorreparação: gossip, regra do docente (só quando isolado) e fallback para o bootstrapper."""

import logging
import socket
import threading
import time
import unittest
from unittest import mock

from ott import config
from ott.bootstrapper import Bootstrapper
from ott.node import Node
from ott.protocol import control
from ott.repair import OverlayRepair, candidates_text
from ott.scheduler import Scheduler
from ott.state import Neighbor, NodeState
from tests.test_metrics import _TestNode, free_tcp_port as free_port, free_udp_port
from tests.test_neighbors import wait_until


class _FakeNeighbors:
    """NeighborManager falso: decide-se à mão quem aceita a ligação e regista-se a ordem dos HELLO."""

    def __init__(self, accepts):
        self.accepts = accepts
        self.attempts = []

    def register_handler(self, msg_type, func):
        pass

    def add_up_listener(self, func):
        pass

    def add_down_listener(self, func):
        pass

    def connect_to(self, neighbor_id, ip, dynamic=False):
        self.attempts.append(neighbor_id)
        return neighbor_id in self.accepts


class RepairDecisionTest(unittest.TestCase):
    """A lógica da reparação, sem rede: RTTs e respostas ao HELLO conhecidos."""

    def make_repair(self, rtts, accepts, degrees=None):
        """degrees: grau de alguns candidatos (os outros ficam com grau desconhecido)."""
        degrees = degrees or {}
        self.state = NodeState("O1")
        self.neighbors = _FakeNeighbors(accepts)
        self.exhausted = []
        now = time.monotonic()
        self.state.overlay_candidates = {
            i: {"ip": f"10.0.0.{n}", "degree": degrees.get(i), "via": {"O9"}, "last_seen": now}
            for n, i in enumerate(rtts, 1)}
        ip_to_rtt = {self.state.overlay_candidates[i]["ip"]: rtt for i, rtt in rtts.items()}
        logger = logging.getLogger("ott.test.repair")
        logger.addHandler(logging.NullHandler())
        logger.propagate = False
        return OverlayRepair(self.state, self.neighbors, Scheduler(logger), logger,
                             lambda: self.exhausted.append(True),
                             measure=lambda ip, *args, **kwargs: ip_to_rtt[ip])

    def test_connects_only_to_the_best(self):
        repair = self.make_repair({"O2": 0.030, "O3": 0.010, "O4": 0.020}, accepts={"O2", "O3", "O4"})
        repair.repair()
        self.assertEqual(self.neighbors.attempts, ["O3"])
        self.assertEqual(self.exhausted, [])

    def test_rejected_tries_next(self):
        repair = self.make_repair({"O2": 0.030, "O3": 0.010, "O4": 0.020}, accepts={"O2"})
        repair.repair()
        self.assertEqual(self.neighbors.attempts, ["O3", "O4", "O2"])
        self.assertEqual(self.exhausted, [])

    def test_degree_one_candidate_of_dead_neighbor_goes_last(self):
        """Erro 1: O2 tem o melhor RTT, mas o seu único vizinho era o nó que falhou."""
        repair = self.make_repair({"O2": 0.001, "O3": 0.010, "O4": 0.020}, accepts={"O2", "O3", "O4"},
                                  degrees={"O2": 1, "O3": 3, "O4": 2})
        repair.repair()
        self.assertEqual(self.neighbors.attempts, ["O3"])

    def test_degree_one_candidate_used_if_no_other_accepts(self):
        repair = self.make_repair({"O2": 0.001, "O3": 0.010}, accepts={"O2"}, degrees={"O2": 1, "O3": 3})
        repair.repair()
        self.assertEqual(self.neighbors.attempts, ["O3", "O2"])
        self.assertEqual(self.exhausted, [])

    def test_unreachable_candidate_is_not_tried(self):
        repair = self.make_repair({"O2": None, "O3": 0.010}, accepts={"O2", "O3"})
        repair.repair()
        self.assertEqual(self.neighbors.attempts, ["O3"])

    def test_nobody_accepts_goes_to_bootstrapper(self):
        repair = self.make_repair({"O2": 0.010, "O3": None}, accepts=set())
        repair.repair()
        self.assertEqual(self.neighbors.attempts, ["O2"])
        self.assertEqual(self.exhausted, [True])

    def test_no_candidates_goes_to_bootstrapper(self):
        repair = self.make_repair({}, accepts=set())
        repair.repair()
        self.assertEqual(self.exhausted, [True])

    def test_stops_if_a_neighbor_came_back(self):
        repair = self.make_repair({"O2": 0.010, "O3": 0.020}, accepts=set())
        returned = Neighbor("O7", "10.0.0.7")
        returned.status = "up"
        self.state.add_neighbor(returned)
        with self.assertLogs(repair.logger, "INFO") as logs:
            repair.repair()
        self.assertIn("reparação cancelada: O7 voltou", "\n".join(logs.output))
        self.assertEqual(self.neighbors.attempts, [])
        self.assertEqual(self.exhausted, [])

    def test_waits_for_pending_incoming_before_giving_up(self):
        """Um candidato que se ligou a mim ao mesmo tempo está pendente: não desistir antes de ele confirmar."""
        repair = self.make_repair({"O2": 0.010}, accepts=set())
        arriving = Neighbor("O2", "10.0.0.2")          # ligação de entrada, à espera do HELLO_CONFIRM
        self.state.add_neighbor(arriving)

        def confirm_later():
            time.sleep(0.2)
            with self.state.lock:
                arriving.status = "up"
        threading.Thread(target=confirm_later, daemon=True).start()
        with self.assertLogs(repair.logger, "INFO") as logs:
            repair.repair()
        self.assertEqual(self.exhausted, [])
        self.assertIn("reparação cancelada: O2 voltou", "\n".join(logs.output))

    def test_does_not_repair_a_node_that_never_had_neighbors(self):
        repair = self.make_repair({"O2": 0.010}, accepts={"O2"})
        repair.periodic_check()
        time.sleep(0.05)
        self.assertEqual(self.neighbors.attempts, [])

    def test_retries_while_isolated(self):
        repair = self.make_repair({"O2": 0.010}, accepts=set())
        repair._on_neighbor_up("O9")
        repair.periodic_check()
        self.assertTrue(wait_until(lambda: len(self.exhausted) == 1 and not repair._repairing))
        repair.periodic_check()
        self.assertTrue(wait_until(lambda: len(self.exhausted) == 2))

    def test_expiry(self):
        repair = self.make_repair({"O2": 0.010}, accepts=set())
        seen = self.state.overlay_candidates["O2"]["last_seen"]
        # Margem de 0,01 s: com o relógio em vírgula flutuante, (seen + 60) - seen pode dar 60,000...01
        repair.expire_candidates(now=seen + config.CANDIDATE_EXPIRY - 0.01)
        self.assertIn("O2", self.state.overlay_candidates)
        repair.expire_candidates(now=seen + config.CANDIDATE_EXPIRY + 1)
        self.assertEqual(self.state.overlay_candidates, {})

    def test_invalid_neighbor_list_is_ignored(self):
        repair = self.make_repair({}, accepts=set())
        repair._on_neighbor_list("O2", control.make_msg(control.NEIGHBOR_LIST, "O2", neighbors="x"))
        repair._on_neighbor_list("O2", control.make_msg(control.NEIGHBOR_LIST, "O2", neighbors=[
            "x", {"id": "O3"}, {"id": "O4", "ip": "isto não é um IP"}, {"id": 5, "ip": "10.0.0.5"},
            {"id": "O1", "ip": "10.0.0.1"},                 # o próprio nó
            {"id": "O6", "ip": "10.0.0.6", "degree": 2},
            {"id": "O7", "ip": "10.0.0.7", "degree": "x"}]))
        self.assertEqual(sorted(self.state.overlay_candidates), ["O6", "O7"])
        self.assertEqual(self.state.overlay_candidates["O6"]["degree"], 2)
        self.assertIsNone(self.state.overlay_candidates["O7"]["degree"])   # grau inválido = desconhecido
        self.assertEqual(self.state.neighbor_degrees["O2"], 7)              # tamanho da lista de O2
        self.assertIn("O6 10.0.0.6 grau 2 indicado por O2", candidates_text(self.state))


class _Lines(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def logged_in(node, text):
    return any(text in line for line in node.log_capture.lines)


class _RepairNode(_TestNode):
    def __init__(self, test, index, control_port, probe_port, measure=None):
        super().__init__(test, index, control_port, probe_port, run_scheduler=False)
        self.log_capture = _Lines()
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.log_capture)
        self.exhausted = []
        extra = {} if measure is None else {"measure": measure}
        self.repair = OverlayRepair(self.state, self.manager, self.scheduler, self.logger,
                                    lambda: self.exhausted.append(True),
                                    bind_ip=self.ip, probe_port=probe_port, **extra)
        self.scheduler.start()

    def stop(self):
        self.repair.stop()
        super().stop()


class RepairIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.control_port = free_port()
        self.probe_port = free_udp_port()
        self.nodes = []

    def tearDown(self):
        for node in self.nodes:
            node.stop()

    def make_node(self, index, measure=None):
        node = _RepairNode(self, index, self.control_port, self.probe_port, measure)
        self.nodes.append(node)
        return node

    def link(self, a, b):
        self.assertTrue(a.manager.connect_to(b.id, b.ip))
        self.assertTrue(wait_until(lambda: b.state.get_neighbor(a.id) is not None))

    def gossip(self, *nodes):
        for node in nodes:
            node.repair.send_gossip()

    def has_candidate(self, node, candidate_id):
        with node.state.lock:
            return candidate_id in node.state.overlay_candidates

    def crash(self, node):
        node.stop()
        self.nodes.remove(node)

    def test_gossip_fills_candidates(self):
        a, b, c = self.make_node(1), self.make_node(2), self.make_node(3)
        self.link(a, b)
        self.link(b, c)
        self.gossip(b)
        self.assertTrue(wait_until(lambda: self.has_candidate(a, c.id) and self.has_candidate(c, a.id)))
        self.assertFalse(self.has_candidate(a, a.id))
        with a.state.lock:
            self.assertEqual(a.state.overlay_candidates[c.id]["via"], {b.id})
            self.assertEqual(a.state.overlay_candidates[c.id]["ip"], c.ip)

    def test_gossip_is_immediate_when_a_neighbor_comes_up(self):
        a, b, c = self.make_node(1), self.make_node(2), self.make_node(3)
        self.link(a, b)
        start = time.monotonic()
        self.link(b, c)  # ninguém chama send_gossip, e o período de gossip ainda não passou
        # O vizinho novo recebe a lista de b, e o vizinho antigo recebe a lista atualizada
        self.assertTrue(wait_until(lambda: self.has_candidate(c, a.id) and self.has_candidate(a, c.id)))
        self.assertLess(time.monotonic() - start, config.GOSSIP_INTERVAL)

    def test_degree_change_is_propagated_without_waiting(self):
        """O grau que chega a um vizinho de 2.º nível acompanha as ligações novas, sem o gossip periódico."""
        a, b, c, d = self.make_node(1), self.make_node(2), self.make_node(3), self.make_node(4)
        self.link(a, b)
        self.link(b, c)   # c tem grau 1 quando b o anuncia a a
        start = time.monotonic()
        self.link(c, d)   # c passa a grau 2: b tem de reenviar a sua lista a a
        self.assertTrue(wait_until(lambda: self.candidate_degree(a, c.id) == 2))
        self.assertLess(time.monotonic() - start, config.GOSSIP_INTERVAL)

    def candidate_degree(self, node, candidate_id):
        with node.state.lock:
            candidate = node.state.overlay_candidates.get(candidate_id)
            return None if candidate is None else candidate["degree"]

    def test_candidates_survive_death_of_indicator(self):
        a, b, c = self.make_node(1), self.make_node(2), self.make_node(3)
        self.link(a, b)
        self.link(b, c)
        self.gossip(b)
        self.assertTrue(wait_until(lambda: self.has_candidate(a, c.id)))
        self.crash(b)
        self.assertTrue(wait_until(lambda: a.state.get_neighbor(b.id) is None))
        self.assertTrue(self.has_candidate(a, c.id))

    def test_t8_isolated_leaves_do_not_form_an_island(self):
        """Caso do T8 (erro 1): O7 e O10 só ligados a O6. Quando O6 cai, O10 não escolhe O7,
        mesmo sendo o candidato com RTT mais baixo."""
        ips = {}

        def measure(ip, *args, **kwargs):
            return 0.0001 if ip == ips.get("O7") else 0.001  # O7 é o mais rápido para toda a gente

        o4, o5, o6, o7, o10 = (self.make_node(i, measure) for i in (4, 5, 6, 7, 10))
        ips["O7"] = o7.ip
        for a, b in ((o4, o5), (o6, o4), (o6, o5), (o6, o7), (o6, o10)):
            self.link(a, b)
        for _ in range(2):  # 1.ª volta: cada nó fica a saber o grau dos vizinhos; 2.ª: propaga-o
            self.gossip(o4, o5, o6, o7, o10)
            time.sleep(0.1)
        with o10.state.lock:
            self.assertEqual(o10.state.overlay_candidates["O7"]["degree"], 1)

        self.crash(o6)
        for leaf in (o7, o10):
            self.assertTrue(wait_until(lambda: leaf.state.count_up() == 1), leaf.id)
        for leaf in (o7, o10):
            self.assertIn(leaf.state.up_neighbors()[0].id, ("O4", "O5"), leaf.id)

    def test_t8_dynamic_edge_closed_when_static_neighbor_returns(self):
        """Caso do T8 (erro 3, regra a): O10 só tem O6. O6 cai, O10 liga-se a O4 (aresta dinâmica).
        Quando O6 volta, O10 fecha a aresta O10-O4 com LEAVE e a topologia volta à do ficheiro."""
        o4, o6, o10 = self.make_node(4), self.make_node(6), self.make_node(10)
        o10.state.static_neighbors = {"O6": o6.ip}
        self.link(o6, o4)
        self.link(o6, o10)
        self.gossip(o6)
        self.assertTrue(wait_until(lambda: self.has_candidate(o10, "O4")))

        self.crash(o6)
        self.assertTrue(wait_until(lambda: o10.state.get_neighbor("O4") is not None
                                   and o10.state.get_neighbor("O4").status == "up"))
        self.assertTrue(wait_until(lambda: o4.state.get_neighbor("O10") is not None))
        self.assertTrue(o10.state.get_neighbor("O4").dynamic)
        self.assertTrue(o4.state.get_neighbor("O10").dynamic)   # marcada nos dois lados

        o6 = self.make_node(6)
        o6.state.static_neighbors = {"O4": o4.ip, "O10": o10.ip}
        self.assertTrue(o6.manager.connect_to("O10", o10.ip))
        self.assertTrue(wait_until(lambda: [n.id for n in o10.state.up_neighbors()] == ["O6"]))
        self.assertTrue(wait_until(lambda: o4.state.get_neighbor("O10") is None))
        self.assertTrue(wait_until(lambda: logged_in(o4, "O10 saiu")))

    def test_t9_full_node_gives_static_neighbor_the_place_of_a_dynamic_edge(self):
        """Caso do T9 (erro 3, regra b), com X_MAX = 2: O5 está cheio com O3 (estático) e O8
        (dinâmico). O4, vizinho estático de O5, volta: O5 fecha a aresta O5-O8 para lhe dar lugar,
        porque O8 tem outro vizinho (O9)."""
        with mock.patch.object(config, "X_MAX", 2):
            o3, o4, o5, o8, o9 = (self.make_node(i) for i in (3, 4, 5, 8, 9))
            o5.state.static_neighbors = {"O3": o3.ip, "O4": o4.ip}
            self.link(o5, o3)
            self.link(o8, o9)
            self.assertTrue(o8.manager.connect_to("O5", o5.ip, dynamic=True))
            self.assertTrue(wait_until(lambda: o5.state.count_up() == 2))
            self.gossip(o8)  # O5 fica a saber que O8 tem grau 2
            self.assertTrue(wait_until(lambda: o5.state.neighbor_degrees.get("O8") == 2))

            self.assertTrue(o4.manager.connect_to("O5", o5.ip))
            self.assertTrue(wait_until(lambda: sorted(n.id for n in o5.state.up_neighbors()) == ["O3", "O4"]))
            self.assertTrue(wait_until(lambda: [n.id for n in o8.state.up_neighbors()] == ["O9"]))

    def test_place_is_only_given_after_confirm(self):
        """Com X_MAX = 2: O5 cheio com O3 e O8 (dinâmico, grau 2). Um HELLO de O4 (estático) sem
        HELLO_CONFIRM não fecha a aresta O5-O8; com CONFIRM, fecha."""
        with mock.patch.object(config, "X_MAX", 2):
            o3, o4, o5, o8, o9 = (self.make_node(i) for i in (3, 4, 5, 8, 9))
            o5.state.static_neighbors = {"O3": o3.ip, "O4": o4.ip}
            self.link(o5, o3)
            self.link(o8, o9)
            self.assertTrue(o8.manager.connect_to("O5", o5.ip, dynamic=True))
            self.gossip(o8)
            self.assertTrue(wait_until(lambda: o5.state.neighbor_degrees.get("O8") == 2))

            raw = socket.create_connection((o5.ip, self.control_port), timeout=3)
            control.send_msg(raw, control.make_msg(control.HELLO, "O4"))
            self.assertEqual(control.recv_msg(raw)["type"], control.HELLO_ACK)
            # Durante o handshake: 3 ligações (X_MAX + 1), a aresta dinâmica continua
            self.assertEqual(sorted(n.id for n in o5.state.up_neighbors()), ["O3", "O8"])
            self.assertEqual(o5.state.get_neighbor("O4").status, "connecting")
            raw.close()  # o CONFIRM nunca chega
            self.assertTrue(wait_until(lambda: o5.state.get_neighbor("O4") is None))
            time.sleep(0.2)
            self.assertEqual(sorted(n.id for n in o5.state.up_neighbors()), ["O3", "O8"])
            self.assertEqual([n.id for n in o8.state.up_neighbors()].count("O5"), 1)

            self.assertTrue(o4.manager.connect_to("O5", o5.ip))   # agora com CONFIRM
            self.assertTrue(wait_until(lambda: sorted(n.id for n in o5.state.up_neighbors()) == ["O3", "O4"]))
            self.assertTrue(wait_until(lambda: [n.id for n in o8.state.up_neighbors()] == ["O9"]))

    def test_t9_full_node_rejects_if_dynamic_edge_would_isolate(self):
        """Regra b: se o outro lado da aresta dinâmica só tem esta ligação, não se fecha: "full"."""
        with mock.patch.object(config, "X_MAX", 2):
            o3, o4, o5, o8 = (self.make_node(i) for i in (3, 4, 5, 8))
            o5.state.static_neighbors = {"O3": o3.ip, "O4": o4.ip}
            self.link(o5, o3)
            self.assertTrue(o8.manager.connect_to("O5", o5.ip, dynamic=True))
            self.gossip(o8)  # O8 só tem O5: grau 1
            self.assertTrue(wait_until(lambda: o5.state.neighbor_degrees.get("O8") == 1))

            self.assertFalse(o4.manager.connect_to("O5", o5.ip))
            self.assertTrue(wait_until(lambda: logged_in(o5, "HELLO de O4 rejeitado (full)")))
            self.assertEqual(sorted(n.id for n in o5.state.up_neighbors()), ["O3", "O8"])

    def test_teacher_example(self):
        """n1 ligado a n2 e n3; n2 ligado a n1 e n4. Sem n3 continua só com n2; sem n2 liga-se a n4."""
        n1, n2, n3, n4 = (self.make_node(i) for i in (1, 2, 3, 4))
        self.link(n1, n2)
        self.link(n1, n3)
        self.link(n2, n4)
        self.gossip(n2)
        self.assertTrue(wait_until(lambda: self.has_candidate(n1, n4.id)))

        self.crash(n3)
        self.assertTrue(wait_until(lambda: n1.state.count_up() == 1))
        time.sleep(0.5)
        self.assertEqual([n.id for n in n1.state.up_neighbors()], [n2.id])

        self.crash(n2)
        self.assertTrue(wait_until(lambda: n1.state.get_neighbor(n4.id) is not None
                                   and n1.state.get_neighbor(n4.id).status == "up"))
        self.assertTrue(wait_until(lambda: n1.state.count_up() == 1))
        self.assertEqual(n1.exhausted, [])


class ReBootstrapTest(unittest.TestCase):
    def make_node(self, port):
        logger = logging.getLogger("ott.test.rebootstrap")
        logger.addHandler(logging.NullHandler())
        logger.propagate = False
        patches = [mock.patch("ott.node.get_logger", return_value=logger),
                   mock.patch.object(config, "BOOTSTRAP_PORT", port)]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        node = Node("O1", "127.0.0.1", "127.0.0.1")
        node.neighbors.connect_async = mock.Mock()
        return node

    def test_asks_bootstrapper_again(self):
        logger = logging.getLogger("ott.test.rebootstrap.b")
        logger.addHandler(logging.NullHandler())
        logger.propagate = False
        nodes = {"O1": {"ip": "127.0.0.1", "neighbors": ["O2"]}, "O2": {"ip": "127.0.0.2", "neighbors": ["O1"]}}
        bootstrapper = Bootstrapper(nodes, logger, bind="127.0.0.1", port=0)
        threading.Thread(target=bootstrapper.serve_forever, daemon=True).start()
        self.addCleanup(bootstrapper.stop)
        node = self.make_node(bootstrapper.port)
        node._rebootstrap()
        self.assertEqual(node.state.static_neighbors, {"O2": "127.0.0.2"})
        node.neighbors.connect_async.assert_called_once_with("O2", "127.0.0.2")

    def test_bootstrapper_down_does_not_raise(self):
        node = self.make_node(free_port())
        node._rebootstrap()
        node.neighbors.connect_async.assert_not_called()


if __name__ == "__main__":
    unittest.main()
