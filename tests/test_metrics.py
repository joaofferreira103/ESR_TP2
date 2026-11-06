"""Testes das métricas: LinkMetrics (contas à mão), packet pair, função de custo e ProbeService entre nós em 127.0.0.x."""

import logging
import socket
import time
import unittest

from ott import config
from ott.cost import link_cost
from ott.metrics import LinkMetrics, ProbeService, measure_rtt, metrics_text
from ott.neighbors import NeighborManager
from ott.protocol import control
from ott.scheduler import Scheduler
from ott.state import NodeState
from ott.transport.tcp import TcpTransport
from tests.test_neighbors import free_port, wait_until

BASE_IP = 40
LATE = config.DEAD_TIMEOUT + 1


def replied_rtts(rtts):
    """LinkMetrics que recebeu uma resposta por cada RTT da lista (em segundos)."""
    m = LinkMetrics(now=0.0)
    t = 0.0
    for seq, rtt in enumerate(rtts):
        m.on_probe_sent(seq, t)
        m.on_reply(seq, t + rtt)
        t += 1.0
    return m


class LinkMetricsTest(unittest.TestCase):
    def test_first_sample(self):
        m = replied_rtts([0.100])
        self.assertAlmostEqual(m.srtt, 0.100)
        self.assertAlmostEqual(m.rttvar, 0.050)

    def test_known_sequence(self):
        # Contas à mão, em ms, com alfa = 0,125 e beta = 0,25; o RTTVAR usa o SRTT anterior
        # RTT 100: srtt = 100; rttvar = 100/2 = 50
        # RTT 120: rttvar = 0,75*50 + 0,25*|120-100| = 42,5; srtt = 0,875*100 + 0,125*120 = 102,5
        # RTT 80: rttvar = 0,75*42,5 + 0,25*|80-102,5| = 37,5; srtt = 0,875*102,5 + 0,125*80 = 99,6875
        m = replied_rtts([0.100, 0.120, 0.080])
        self.assertAlmostEqual(m.srtt, 0.0996875)
        self.assertAlmostEqual(m.rttvar, 0.0375)
        self.assertEqual(m.samples, 3)

    def test_loss_10_of_50(self):
        m = LinkMetrics(now=0.0)
        for seq in range(50):
            m.on_probe_sent(seq, 0.0)
            if seq % 5 != 0:  # 10 dos 50 ficam sem resposta
                m.on_reply(seq, 0.01)
        self.assertAlmostEqual(m.loss(now=LATE), 0.2)

    def test_pending_probe_is_not_lost_yet(self):
        m = LinkMetrics(now=0.0)
        m.on_probe_sent(1, 0.0)
        self.assertEqual(m.loss(now=config.DEAD_TIMEOUT), 0.0)
        self.assertEqual(m.loss(now=config.DEAD_TIMEOUT + 0.01), 1.0)

    def test_old_probes_leave_window(self):
        m = LinkMetrics(now=0.0)
        for seq in range(10):  # 10 perdidos, a sair da janela
            m.on_probe_sent(seq, 0.0)
        self.assertEqual(m.loss(now=LATE), 1.0)
        for seq in range(10, 10 + config.LOSS_WINDOW):
            m.on_probe_sent(seq, LATE)
            m.on_reply(seq, LATE + 0.01)
        self.assertEqual(m.loss(now=LATE + 1), 0.0)
        self.assertEqual(len(m._probes), config.LOSS_WINDOW)

    def test_late_duplicate_and_unknown_replies_ignored(self):
        m = replied_rtts([0.100])
        srtt = m.srtt
        m.on_reply(0, 5.0)              # duplicado
        m.on_reply(999, 5.0)            # nunca enviado
        m.on_probe_sent(1, 10.0)
        m.on_reply(1, 10.0 + LATE)      # fora de tempo: já contou como perdido
        self.assertEqual(m.samples, 1)
        self.assertEqual(m.srtt, srtt)
        self.assertEqual(m.loss(now=10.0 + LATE), 0.5)

    def test_is_ready(self):
        self.assertFalse(replied_rtts([0.01] * (config.MIN_SAMPLES - 1)).is_ready())
        self.assertTrue(replied_rtts([0.01] * config.MIN_SAMPLES).is_ready())

    def test_snapshot_in_ms(self):
        s = replied_rtts([0.100]).snapshot(now=1.0)
        self.assertAlmostEqual(s["srtt"], 100.0)
        self.assertAlmostEqual(s["rttvar"], 50.0)
        self.assertEqual((s["loss"], s["samples"]), (0.0, 1))
        self.assertIsNone(LinkMetrics(now=0.0).snapshot(now=0.0)["srtt"])

    def test_silent_for(self):
        m = LinkMetrics(now=10.0)
        self.assertEqual(m.silent_for(12.0), 2.0)  # link novo: conta desde a criação
        m.on_probe_sent(1, 12.0)
        m.on_reply(1, 12.5)
        self.assertEqual(m.silent_for(13.0), 0.5)


def pair_capacity_interval(bits_per_second):
    """Intervalo entre as chegadas de um par num link com esta capacidade."""
    return config.PAIR_SIZE * 8 / bits_per_second


def add_pair(metrics, pair_id, bits_per_second, t=100.0):
    metrics.on_pair_packet(pair_id, 0, t)
    metrics.on_pair_packet(pair_id, 1, t + pair_capacity_interval(bits_per_second))


class PacketPairTest(unittest.TestCase):
    def test_known_interval(self):
        m = LinkMetrics(now=0.0)
        self.assertIsNone(m.capacity())
        # 1200 bytes em 1 ms: 1200 * 8 / 0,001 = 9,6 Mbit/s
        m.on_pair_packet(1, 0, 10.0)
        m.on_pair_packet(1, 1, 10.001)
        self.assertAlmostEqual(m.capacity(), config.PAIR_SIZE * 8 / 0.001, delta=1)
        self.assertAlmostEqual(m.capacity(), 9.6e6, delta=1)

    def test_median_ignores_extreme(self):
        m = LinkMetrics(now=0.0)
        for pair_id, capacity in enumerate([10e6, 11e6, 9e6, 10.5e6, 1e12]):
            add_pair(m, pair_id, capacity)
        self.assertAlmostEqual(m.capacity(), 10.5e6, delta=1000)

    def test_history_keeps_only_last(self):
        m = LinkMetrics(now=0.0)
        for pair_id in range(config.PAIR_HISTORY):
            add_pair(m, pair_id, 1e6)
        for pair_id in range(100, 100 + config.PAIR_HISTORY):
            add_pair(m, pair_id, 50e6)
        self.assertAlmostEqual(m.capacity(), 50e6, delta=1000)

    def test_incomplete_pair_discarded(self):
        m = LinkMetrics(now=0.0)
        m.on_pair_packet(1, 0, 10.0)            # falta o idx 1
        self.assertIsNone(m.capacity())
        m.on_pair_packet(2, 1, 10.1)            # falta o idx 0: não pode juntar-se ao par 1
        self.assertIsNone(m.capacity())
        m.on_pair_packet(3, 0, 11.0)            # idx 0 sem par, substituído pelo do par seguinte
        m.on_pair_packet(4, 0, 12.0)
        m.on_pair_packet(4, 1, 12.001)
        self.assertAlmostEqual(m.capacity(), 9.6e6, delta=1)

    def test_swapped_pair_discarded(self):
        m = LinkMetrics(now=0.0)
        m.on_pair_packet(1, 1, 10.0)
        m.on_pair_packet(1, 0, 10.001)
        self.assertIsNone(m.capacity())

    def test_zero_interval_discarded(self):
        m = LinkMetrics(now=0.0)
        m.on_pair_packet(1, 0, 10.0)
        m.on_pair_packet(1, 1, 10.0)
        self.assertIsNone(m.capacity())

    def test_throughput(self):
        m = LinkMetrics(now=0.0)
        self.assertEqual(m.throughput(now=0.0), 0)
        m.record_data_bytes(1000, 1.0)
        m.record_data_bytes(2000, 2.0)
        self.assertAlmostEqual(m.throughput(now=2.0), 3000 * 8 / config.BW_WINDOW)
        # O primeiro registo sai da janela
        self.assertAlmostEqual(m.throughput(now=1.0 + config.BW_WINDOW + 0.1), 2000 * 8 / config.BW_WINDOW)


class LinkCostTest(unittest.TestCase):
    NOW = 1000.0

    def ready(self, srtt=0.100, rttvar=0.020, lost=0, capacity=None):
        """Link pronto com valores exatos: lost probes perdidos em LOSS_WINDOW."""
        m = LinkMetrics(now=0.0)
        m.srtt, m.rttvar, m.samples = srtt, rttvar, config.MIN_SAMPLES
        for seq in range(config.LOSS_WINDOW):
            m.on_probe_sent(seq, 0.0)
            if seq >= lost:
                m.on_reply(seq, 0.001)
        m.srtt, m.rttvar, m.samples = srtt, rttvar, config.MIN_SAMPLES  # on_reply alterou-os
        if capacity is not None:
            add_pair(m, 1, capacity)
        return m

    def cost(self, m, mode="full"):
        return link_cost(m, mode, now=self.NOW)

    def test_full_known_value(self):
        # 100/2 + 1*20 + 100*0,2 + 10*2/4 + 1 = 50 + 20 + 20 + 5 + 1 = 96 ms
        self.assertAlmostEqual(self.cost(self.ready(lost=10, capacity=4e6)), 96.0, places=3)

    def test_ten_percent_loss_adds_ten_ms(self):
        # 50 + 20 + 100*0,1 + 0 + 1 = 81 ms
        self.assertAlmostEqual(self.cost(self.ready(lost=5)), 81.0)

    def test_capacity_equal_to_required_adds_ten_ms(self):
        # 50 + 20 + 0 + 10*2/2 + 1 = 81 ms
        self.assertAlmostEqual(self.cost(self.ready(capacity=2e6)), 81.0, places=3)

    def test_loss_counts_when_delay_is_almost_zero(self):
        # Com perda: 0,1 + 0,1 + 100*0,1 + 1 = 11,2 ms. Limpo: 5 + 0,1 + 1 = 6,1 ms
        lossy = self.cost(self.ready(srtt=0.0002, rttvar=0.0001, lost=5))
        clean = self.cost(self.ready(srtt=0.010, rttvar=0.0001))
        self.assertAlmostEqual(lossy, 11.2)
        self.assertAlmostEqual(clean, 6.1)
        self.assertGreater(lossy, clean)

    def test_full_unknown_capacity_has_factor_one(self):
        # 50 + 20 + 0 + 0 + 1 = 71 ms
        self.assertAlmostEqual(self.cost(self.ready()), 71.0)

    def test_latency(self):
        self.assertAlmostEqual(self.cost(self.ready(lost=10, capacity=4e6), "latency"), 51.0)

    def test_hops(self):
        self.assertEqual(self.cost(self.ready(lost=10, capacity=4e6), "hops"), config.HOP_COST)

    def test_not_ready_is_default_in_every_mode(self):
        m = replied_rtts([0.001] * (config.MIN_SAMPLES - 1))
        for mode in ("full", "latency", "hops"):
            self.assertEqual(self.cost(m, mode), config.DEFAULT_LINK_COST)

    def test_unknown_mode(self):
        with self.assertRaises(ValueError):
            self.cost(self.ready(), "pombo")

    def test_more_loss_costs_more(self):
        costs = [self.cost(self.ready(lost=lost)) for lost in (0, 1, 10, 25, 50)]
        self.assertEqual(costs, sorted(set(costs)))

    def test_more_jitter_costs_more(self):
        costs = [self.cost(self.ready(rttvar=v)) for v in (0.0, 0.001, 0.02, 0.5)]
        self.assertEqual(costs, sorted(set(costs)))

    def test_less_capacity_costs_more(self):
        costs = [self.cost(self.ready(capacity=c)) for c in (1e9, 100e6, 10e6, 2e6, 0.5e6)]
        self.assertEqual(costs, sorted(set(costs)))

    def test_cost_always_positive(self):
        self.assertGreater(self.cost(self.ready(srtt=0.0, rttvar=0.0)), 0)


def free_udp_port():
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        try:
            for index in range(1, 8):
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.bind((f"127.0.0.{BASE_IP + index}", port))
            return port
        except OSError:
            continue


class _TestNode:
    """Um nó de teste: estado, gestor de vizinhos, scheduler e ProbeService."""

    def __init__(self, test, index, control_port, probe_port, run_scheduler=True):
        self.id = f"O{index}"
        self.ip = f"127.0.0.{BASE_IP + index}"
        self.logger = logging.getLogger(f"ott.test.{test.id()}.{self.id}")
        self.logger.propagate = False
        self.logger.addHandler(logging.NullHandler())
        self.state = NodeState(self.id)
        factory = lambda on_message, on_closed: TcpTransport(on_message, on_closed, self.logger, self.ip)
        self.manager = NeighborManager(self.state, self.logger, factory, port=control_port)
        self.scheduler = Scheduler(self.logger)
        self.probes = ProbeService(self.state, self.manager, self.scheduler, self.logger,
                                   bind_ip=self.ip, port=probe_port)
        self.probes.start()
        self.manager.start()
        if run_scheduler:
            self.scheduler.start()

    def metrics(self, neighbor_id):
        with self.state.lock:
            return self.state.link_metrics.get(neighbor_id)

    def stop(self):
        self.scheduler.stop()
        self.manager.stop()
        self.probes.stop()


class ProbeServiceTest(unittest.TestCase):
    def setUp(self):
        self.control_port = free_port()
        self.probe_port = free_udp_port()
        self.nodes = []

    def tearDown(self):
        for node in self.nodes:
            node.stop()

    def make_node(self, index, **kwargs):
        node = _TestNode(self, index, self.control_port, self.probe_port, **kwargs)
        self.nodes.append(node)
        return node

    def connected_pair(self):
        a, b = self.make_node(1), self.make_node(2)
        self.assertTrue(a.manager.connect_to(b.id, b.ip))
        self.assertTrue(wait_until(lambda: b.state.count_up() == 1))
        return a, b

    def test_both_ready_after_two_seconds(self):
        a, b = self.connected_pair()
        time.sleep(2)
        for node, peer in ((a, b.id), (b, a.id)):
            metrics = node.metrics(peer)
            self.assertTrue(metrics.is_ready())
            self.assertGreater(metrics.srtt, 0)
            self.assertLess(metrics.srtt, 0.1)
            self.assertEqual(metrics.loss(), 0.0)
        self.assertIn("amostras=", metrics_text(a.state))
        self.assertIn(b.id, metrics_text(a.state))

    def test_pair_measures_sender_to_receiver_direction(self):
        a, b = self.connected_pair()
        self.assertTrue(wait_until(lambda: b.metrics(a.id) is not None))
        a.probes.send_pairs()  # só a envia
        self.assertTrue(wait_until(lambda: b.metrics(a.id).capacity() is not None))
        self.assertGreater(b.metrics(a.id).capacity(), 0)
        self.assertIsNone(a.metrics(b.id).capacity())
        text = metrics_text(b.state)
        self.assertIn("capacidade=", text)
        self.assertIn("custo=", text)

    def test_pair_datagrams_have_pair_size(self):
        a = self.make_node(1, run_scheduler=False)
        first, second = a.probes._pair_datagrams(123456)
        self.assertEqual((len(first), len(second)), (config.PAIR_SIZE, config.PAIR_SIZE))
        msgs = [control.decode_udp(first), control.decode_udp(second)]
        self.assertEqual([m["idx"] for m in msgs], [0, 1])
        self.assertEqual([m["pair_id"] for m in msgs], [123456, 123456])
        self.assertEqual({m["type"] for m in msgs}, {control.PROBE_PAIR})

    def test_silent_neighbor_declared_dead(self):
        a, b = self.connected_pair()
        downs = []
        a.manager.add_down_listener(lambda nid, reason: downs.append((nid, reason, time.monotonic())))
        self.assertTrue(wait_until(lambda: a.metrics(b.id).samples > 0))
        start = time.monotonic()
        b.probes.stop()  # o nó deixa de responder, mas a ligação TCP continua aberta
        self.assertTrue(wait_until(lambda: downs, timeout=config.DEAD_TIMEOUT + 2))
        neighbor_id, reason, when = downs[0]
        self.assertEqual((neighbor_id, reason), (b.id, "sem resposta aos probes"))
        # O último probe respondido pode ser até um intervalo anterior a start; a verificação corre a cada intervalo
        self.assertGreater(when - start, config.DEAD_TIMEOUT - 2 * config.PROBE_INTERVAL)
        self.assertLess(when - start, config.DEAD_TIMEOUT + 1.0)
        self.assertEqual(a.state.count_up(), 0)
        self.assertIsNone(a.metrics(b.id))  # removidas em on_neighbor_down
        self.assertEqual(len(downs), 1)

    def test_metrics_created_and_removed_with_neighbor(self):
        a, b = self.connected_pair()
        self.assertIsNotNone(a.metrics(b.id))
        self.assertTrue(wait_until(lambda: b.metrics(a.id) is not None))
        a.stop()
        self.nodes.remove(a)
        self.assertTrue(wait_until(lambda: b.metrics(a.id) is None))

    def test_measure_rtt_to_non_neighbor(self):
        node = self.make_node(1)
        self.assertEqual(node.state.count_up(), 0)
        rtt = measure_rtt(node.ip, config.REPAIR_PROBES, 1.0, node_id="O9", port=self.probe_port)
        self.assertIsNotNone(rtt)
        self.assertGreater(rtt, 0)
        self.assertLess(rtt, 1.0)

    def test_measure_rtt_bad_bind_returns_none(self):
        self.assertIsNone(measure_rtt("127.0.0.1", 1, 0.1, bind_ip="192.0.2.1"))

    def test_measure_rtt_without_answer(self):
        start = time.monotonic()
        self.assertIsNone(measure_rtt("127.0.0.43", 2, 0.2, port=self.probe_port))
        self.assertLess(time.monotonic() - start, 1.0)

    def test_reply_identified_by_src_not_ip(self):
        a = self.make_node(1, run_scheduler=False)
        with a.state.lock:
            metrics = LinkMetrics()
            a.state.link_metrics["O2"] = metrics
            metrics.on_probe_sent(7, time.monotonic())
        # A resposta chega de um IP que não tem nada a ver com o vizinho
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("127.0.0.99", 0))
            reply = control.make_msg(control.PROBE_REPLY, "O2", seq=7, ts=0)
            s.sendto(control.encode_udp(reply), (a.ip, self.probe_port))
        self.assertTrue(wait_until(lambda: metrics.samples == 1))

    def test_reply_from_unknown_node_ignored_and_bad_datagrams_survive(self):
        a = self.make_node(1, run_scheduler=False)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            target = (a.ip, self.probe_port)
            s.sendto(b"isto nao e json", target)
            s.sendto(control.encode_udp({"v": 1, "type": "PROBE", "src": "X", "seq": "abc"}), target)
            s.sendto(control.encode_udp(control.make_msg(control.PROBE_REPLY, "O9", seq=1, ts=0)), target)
            s.sendto(control.encode_udp(control.make_msg(control.PROBE_PAIR, "O9", pair_id=1, idx=0, pad="")), target)
            s.sendto(control.encode_udp(control.make_msg(control.PROBE_PAIR, "O9", pair_id=1, idx=7, pad="")), target)
        # A thread de receção continua viva e a responder
        self.assertIsNotNone(measure_rtt(a.ip, 1, 1.0, port=self.probe_port))
        self.assertEqual(a.state.link_metrics, {})

    def test_port_in_use_is_an_error(self):
        self.make_node(1)
        with self.assertRaises(OSError):
            self.make_node(1)


if __name__ == "__main__":
    unittest.main()
