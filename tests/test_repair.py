"""Testes da autorreparação: gossip, regra do docente (só quando isolado) e fallback para o bootstrapper."""

import logging
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
from tests.test_metrics import _TestNode, free_udp_port
from tests.test_neighbors import free_port, wait_until


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

    def connect_to(self, neighbor_id, ip):
        self.attempts.append(neighbor_id)
        return neighbor_id in self.accepts


class RepairDecisionTest(unittest.TestCase):
    """A lógica da reparação, sem rede: RTTs e respostas ao HELLO conhecidos."""

    def make_repair(self, rtts, accepts):
        self.state = NodeState("O1")
        self.neighbors = _FakeNeighbors(accepts)
        self.exhausted = []
        now = time.monotonic()
        self.state.overlay_candidates = {
            i: {"ip": f"10.0.0.{n}", "via": "O9", "last_seen": now} for n, i in enumerate(rtts, 1)}
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
        repair.repair()
        self.assertEqual(self.neighbors.attempts, [])
        self.assertEqual(self.exhausted, [])

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
        repair.expire_candidates(now=seen + config.CANDIDATE_EXPIRY)
        self.assertIn("O2", self.state.overlay_candidates)
        repair.expire_candidates(now=seen + config.CANDIDATE_EXPIRY + 1)
        self.assertEqual(self.state.overlay_candidates, {})

    def test_invalid_neighbor_list_is_ignored(self):
        repair = self.make_repair({}, accepts=set())
        repair._on_neighbor_list("O2", control.make_msg(control.NEIGHBOR_LIST, "O2", neighbors="x"))
        repair._on_neighbor_list("O2", control.make_msg(control.NEIGHBOR_LIST, "O2", neighbors=[
            "x", {"id": "O3"}, {"id": "O4", "ip": "isto não é um IP"}, {"id": 5, "ip": "10.0.0.5"},
            {"id": "O1", "ip": "10.0.0.1"},                 # o próprio nó
            {"id": "O6", "ip": "10.0.0.6"}]))
        self.assertEqual(list(self.state.overlay_candidates), ["O6"])
        self.assertIn("O6 10.0.0.6 indicado por O2", candidates_text(self.state))


class _RepairNode(_TestNode):
    def __init__(self, test, index, control_port, probe_port):
        super().__init__(test, index, control_port, probe_port, run_scheduler=False)
        self.exhausted = []
        self.repair = OverlayRepair(self.state, self.manager, self.scheduler, self.logger,
                                    lambda: self.exhausted.append(True),
                                    bind_ip=self.ip, probe_port=probe_port)
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

    def make_node(self, index):
        node = _RepairNode(self, index, self.control_port, self.probe_port)
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
            self.assertEqual(a.state.overlay_candidates[c.id]["via"], b.id)
            self.assertEqual(a.state.overlay_candidates[c.id]["ip"], c.ip)

    def test_gossip_is_immediate_when_a_neighbor_comes_up(self):
        a, b, c = self.make_node(1), self.make_node(2), self.make_node(3)
        self.link(a, b)
        start = time.monotonic()
        self.link(b, c)  # ninguém chama send_gossip, e o período de gossip ainda não passou
        # O vizinho novo recebe a lista de b, e o vizinho antigo recebe a lista atualizada
        self.assertTrue(wait_until(lambda: self.has_candidate(c, a.id) and self.has_candidate(a, c.id)))
        self.assertLess(time.monotonic() - start, config.GOSSIP_INTERVAL)

    def test_candidates_survive_death_of_indicator(self):
        a, b, c = self.make_node(1), self.make_node(2), self.make_node(3)
        self.link(a, b)
        self.link(b, c)
        self.gossip(b)
        self.assertTrue(wait_until(lambda: self.has_candidate(a, c.id)))
        self.crash(b)
        self.assertTrue(wait_until(lambda: a.state.get_neighbor(b.id) is None))
        self.assertTrue(self.has_candidate(a, c.id))

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
