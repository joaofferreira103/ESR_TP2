"""Testes dos protocolos de controlo (TCP/UDP) e de dados (UDP)."""

import json
import socket
import threading
import time
import unittest

from ott import config
from ott.protocol import control, data
from ott.protocol.control import ProtocolError


class ControlProtocolTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = socket.socketpair()
        # Evita que um teste com erro fique bloqueado para sempre num recv
        self.b.settimeout(5)

    def tearDown(self):
        self.a.close()
        self.b.close()

    def test_round_trip(self):
        messages = [
            control.make_msg(control.REGISTER, "O1"),
            control.make_msg(control.NEIGHBORS, "BOOT", neighbors=[{"id": "O2", "ip": "10.0.0.2"}]),
            control.make_msg(control.HELLO_REJECT, "O3", reason="full"),
            control.make_msg(control.ANNOUNCE, "O4", server="S1", epoch=1700000000000, seq=42,
                             streams={"filmeÁ": {"cost": 18.5, "hops": 3, "active": True, "via": "O2"}}),
        ]
        for msg in messages:
            control.send_msg(self.a, msg)
        for msg in messages:
            received = control.recv_msg(self.b)
            self.assertEqual(received, msg)
            control.validate(received)

    def test_partial_reads(self):
        msg = control.make_msg(control.PING, "S1", server="S1", flow="S1/filmeA", ping_id=7)
        body = json.dumps(msg).encode("utf-8")
        raw = control.LENGTH_PREFIX.pack(len(body)) + body

        def send_in_pieces():
            for i in range(0, len(raw), 3):
                self.a.sendall(raw[i:i + 3])
                time.sleep(0.001)

        sender = threading.Thread(target=send_in_pieces)
        sender.start()
        received = control.recv_msg(self.b)
        sender.join()
        self.assertEqual(received, msg)

    def test_closed_connection_returns_none(self):
        self.a.close()
        self.assertIsNone(control.recv_msg(self.b))

    def test_closed_mid_message_returns_none(self):
        self.a.sendall(control.LENGTH_PREFIX.pack(100) + b'{"v": 1')
        self.a.close()
        self.assertIsNone(control.recv_msg(self.b))

    def test_too_large_raises(self):
        self.a.sendall(control.LENGTH_PREFIX.pack(config.MAX_CONTROL_MSG + 1))
        with self.assertRaises(ProtocolError):
            control.recv_msg(self.b)

    def test_send_too_large_raises(self):
        msg = control.make_msg(control.HELLO, "O1", padding="x" * config.MAX_CONTROL_MSG)
        with self.assertRaises(ProtocolError):
            control.send_msg(self.a, msg)

    def test_invalid_json_raises(self):
        body = b"isto nao e json"
        self.a.sendall(control.LENGTH_PREFIX.pack(len(body)) + body)
        with self.assertRaises(ProtocolError):
            control.recv_msg(self.b)

    def test_validate(self):
        control.validate(control.make_msg(control.HELLO, "O1"))
        with self.assertRaises(ProtocolError):
            control.validate({"v": config.PROTOCOL_VERSION + 1, "type": control.HELLO, "src": "O1"})
        with self.assertRaises(ProtocolError):
            control.validate({"v": config.PROTOCOL_VERSION, "type": control.HELLO})
        with self.assertRaises(ProtocolError):
            control.validate(["não", "é", "dict"])

    def test_udp_round_trip(self):
        msg = control.make_msg(control.PROBE, "O5", seq=3, ts=123.456)
        received = control.decode_udp(control.encode_udp(msg))
        self.assertEqual(received, msg)
        control.validate(received)
        with self.assertRaises(ProtocolError):
            control.decode_udp(b"\xff\xfe lixo")


class DataProtocolTest(unittest.TestCase):
    def test_round_trip(self):
        raw = data.pack_data("O2", "S1/filmeA", 1234, 1700000000123, config.TTL, b"\x00\x01quadro")
        p = data.unpack_data(raw)
        self.assertEqual(p.version, config.PROTOCOL_VERSION)
        self.assertEqual(p.type, data.TYPE_DATA)
        self.assertEqual(p.ttl, config.TTL)
        self.assertEqual(p.seq, 1234)
        self.assertEqual(p.timestamp_ms, 1700000000123)
        self.assertEqual(p.src, "O2")
        self.assertEqual(p.flow, "S1/filmeA")
        self.assertEqual(p.payload, b"\x00\x01quadro")

    def test_accents_and_empty_payload(self):
        p = data.unpack_data(data.pack_data("Ó7", "S2/ação_çãé", 0, 0, 1, b""))
        self.assertEqual(p.src, "Ó7")
        self.assertEqual(p.flow, "S2/ação_çãé")
        self.assertEqual(p.payload, b"")

    def test_name_too_long_raises(self):
        with self.assertRaises(ProtocolError):
            data.pack_data("O1", "x" * 256, 0, 0, 1, b"")
        # 128 caracteres acentuados = 256 bytes em UTF-8: o limite é em bytes, não em caracteres
        with self.assertRaises(ProtocolError):
            data.pack_data("é" * 128, "S1/a", 0, 0, 1, b"")

    def test_invalid_packets_raise(self):
        raw = data.pack_data("O1", "S1/a", 0, 0, 1, b"")
        with self.assertRaises(ProtocolError):
            data.unpack_data(raw[:5])
        with self.assertRaises(ProtocolError):
            data.unpack_data(raw[:-2])  # flow truncado
        wrong_version = bytes([config.PROTOCOL_VERSION + 1]) + raw[1:]
        with self.assertRaises(ProtocolError):
            data.unpack_data(wrong_version)

    def test_out_of_range_field_raises(self):
        with self.assertRaises(ProtocolError):
            data.pack_data("O1", "S1/a", 2 ** 32, 0, 1, b"")

    def test_forward_packet(self):
        payload = bytes(range(256)) * 10
        raw = data.pack_data("O1", "S1/filmeA", 99, 555, 5, payload)
        p = data.unpack_data(data.forward_packet(raw, "O2"))
        self.assertEqual(p.ttl, 4)
        self.assertEqual(p.src, "O2")
        self.assertEqual(p.flow, "S1/filmeA")
        self.assertEqual(p.seq, 99)
        self.assertEqual(p.timestamp_ms, 555)
        self.assertEqual(p.payload, payload)

    def test_forward_ttl_zero_raises(self):
        raw = data.pack_data("O1", "S1/filmeA", 0, 0, 0, b"x")
        with self.assertRaises(ProtocolError):
            data.forward_packet(raw, "O2")


if __name__ == "__main__":
    unittest.main()
