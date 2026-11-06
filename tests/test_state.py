"""Testes do estado partilhado."""

import time
import unittest

from ott.state import Neighbor, NodeState


class NodeStateTest(unittest.TestCase):
    def test_add_remove(self):
        state = NodeState("O1")
        n2 = Neighbor("O2", "10.0.0.2")
        self.assertTrue(state.add_neighbor(n2))
        self.assertFalse(state.add_neighbor(Neighbor("O2", "10.0.0.99")))
        self.assertIs(state.get_neighbor("O2"), n2)
        self.assertIs(state.remove_neighbor("O2"), n2)
        self.assertIsNone(state.remove_neighbor("O2"))
        self.assertIsNone(state.get_neighbor("O2"))

    def test_up_neighbors_is_copy(self):
        state = NodeState("O1")
        for i, status in [(2, "up"), (3, "connecting"), (4, "up")]:
            n = Neighbor(f"O{i}", f"10.0.0.{i}")
            n.status = status
            state.add_neighbor(n)
        up = state.up_neighbors()
        self.assertEqual(sorted(n.id for n in up), ["O2", "O4"])
        self.assertEqual(state.count_up(), 2)
        # Alterar a lista devolvida não altera o estado
        up.clear()
        self.assertEqual(state.count_up(), 2)
        # Remover um vizinho não altera uma lista já devolvida
        up = state.up_neighbors()
        state.remove_neighbor("O2")
        self.assertEqual(len(up), 2)

    def test_snapshot(self):
        state = NodeState("O1")
        state.static_neighbors = {"O2": "10.0.0.2", "O3": "10.0.0.3"}
        n = Neighbor("O2", "10.0.0.2", static=True)
        n.status = "up"
        n.connected_since = time.time()
        state.add_neighbor(n)
        text = state.snapshot()
        self.assertIn("O2", text)
        self.assertIn("estáticos sem ligação: O3", text)


if __name__ == "__main__":
    unittest.main()
