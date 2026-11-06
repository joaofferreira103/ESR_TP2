"""Testes do bootstrapper: validação da topologia e resposta a REGISTER."""

import json
import logging
import os
import socket
import tempfile
import threading
import unittest

from ott.bootstrapper import Bootstrapper, TopologyError, load_topology
from ott.protocol import control

CONFIGS = os.path.join(os.path.dirname(__file__), "..", "configs")

# Os avisos esperados nos testes não devem aparecer no terminal (assertLogs continua a apanhá-los)
logging.getLogger("ott.test.bootstrapper").addHandler(logging.NullHandler())

TOPOLOGY = {
    "nodes": {
        "O1": {"ip": "127.0.0.11", "neighbors": ["O2"]},
        "O2": {"ip": "127.0.0.12", "neighbors": ["O1", "O3"]},
        "O3": {"ip": "127.0.0.13", "neighbors": ["O2"]},
    }
}


def write_topology(data):
    """Escreve a topologia num ficheiro temporário e devolve o caminho."""
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    json.dump(data, f)
    f.close()
    return f.name


class LoadTopologyTest(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("ott.test.bootstrapper")
        self.paths = []

    def tearDown(self):
        for path in self.paths:
            os.remove(path)

    def load(self, data):
        path = write_topology(data)
        self.paths.append(path)
        return load_topology(path, self.logger)

    def test_valid(self):
        nodes = self.load(TOPOLOGY)
        self.assertEqual(nodes["O2"]["neighbors"], ["O1", "O3"])

    def test_unknown_neighbor(self):
        with self.assertRaises(TopologyError):
            self.load({"nodes": {"O1": {"ip": "127.0.0.11", "neighbors": ["O9"]}}})

    def test_invalid_ip(self):
        with self.assertRaises(TopologyError):
            self.load({"nodes": {"O1": {"ip": "<IP_O1>", "neighbors": []}}})

    def test_self_neighbor(self):
        with self.assertRaises(TopologyError):
            self.load({"nodes": {"O1": {"ip": "127.0.0.11", "neighbors": ["O1"]}}})

    def test_asymmetric_warns(self):
        data = {"nodes": {
            "O1": {"ip": "127.0.0.11", "neighbors": ["O2"]},
            "O2": {"ip": "127.0.0.12", "neighbors": []},
        }}
        with self.assertLogs(self.logger, level="WARNING") as logs:
            self.load(data)
        self.assertIn("não simétrica", logs.output[0])

    def test_local_configs_are_valid(self):
        for name in ("local_cenario1.json", "local_cenario2.json"):
            load_topology(os.path.join(CONFIGS, name), self.logger)


class BootstrapperServerTest(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("ott.test.bootstrapper")
        self.bootstrapper = Bootstrapper(TOPOLOGY["nodes"], self.logger, bind="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.bootstrapper.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.bootstrapper.stop()
        self.thread.join(2)

    def request(self, msg):
        with socket.create_connection(("127.0.0.1", self.bootstrapper.port), timeout=5) as s:
            control.send_msg(s, msg)
            reply = control.recv_msg(s)
            # O bootstrapper fecha a ligação depois de responder
            self.assertIsNone(control.recv_msg(s))
            return reply

    def test_known_node(self):
        reply = self.request(control.make_msg(control.REGISTER, "O2"))
        control.validate(reply)
        self.assertEqual(reply["type"], control.NEIGHBORS)
        self.assertEqual(reply["neighbors"], [{"id": "O1", "ip": "127.0.0.11"},
                                              {"id": "O3", "ip": "127.0.0.13"}])

    def test_unknown_node(self):
        reply = self.request(control.make_msg(control.REGISTER, "O99"))
        self.assertEqual(reply["type"], control.ERROR)
        self.assertEqual(reply["reason"], "unknown node")

    def test_wrong_message_type(self):
        reply = self.request(control.make_msg(control.HELLO, "O1"))
        self.assertEqual(reply["type"], control.ERROR)


if __name__ == "__main__":
    unittest.main()
