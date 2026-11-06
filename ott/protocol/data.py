"""Protocolo de dados: codificação e descodificação do cabeçalho binário dos pacotes UDP.

Formato (big-endian):
  versão (u8) | tipo (u8) | TTL (u8) | seq (u32) | timestamp_ms (u64)
  | src_len (u8) | src (UTF-8) | flow_len (u8) | flow (UTF-8) | payload
"""

import struct
from collections import namedtuple

from ott import config
from ott.protocol.control import ProtocolError

TYPE_DATA = 1

# Parte fixa do cabeçalho: versão, tipo, TTL, seq, timestamp_ms (15 bytes)
FIXED_HEADER = struct.Struct("!BBBIQ")
# Prefixo de tamanho de src e de flow
NAME_LENGTH = struct.Struct("!B")
MAX_NAME_BYTES = 255

DataPacket = namedtuple(
    "DataPacket", ["version", "type", "ttl", "seq", "timestamp_ms", "src", "flow", "payload"]
)


def _encode_name(name, field):
    encoded = name.encode("utf-8")
    if len(encoded) > MAX_NAME_BYTES:
        raise ProtocolError(f"{field} demasiado longo: {len(encoded)} bytes (máximo {MAX_NAME_BYTES})")
    return NAME_LENGTH.pack(len(encoded)) + encoded


def _decode_name(packet, offset, field):
    """Lê um nome com prefixo de 1 byte a partir de offset. Devolve (nome, novo_offset)."""
    if offset + NAME_LENGTH.size > len(packet):
        raise ProtocolError(f"pacote truncado: falta o tamanho de {field}")
    (length,) = NAME_LENGTH.unpack_from(packet, offset)
    offset += NAME_LENGTH.size
    if offset + length > len(packet):
        raise ProtocolError(f"pacote truncado: {field} incompleto")
    try:
        name = packet[offset:offset + length].decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError(f"{field} não é UTF-8 válido") from e
    return name, offset + length


def pack_data(src, flow, seq, timestamp_ms, ttl, payload):
    """Constrói um pacote de dados. src é o nó que envia ESTE salto."""
    try:
        header = FIXED_HEADER.pack(config.PROTOCOL_VERSION, TYPE_DATA, ttl, seq, timestamp_ms)
    except struct.error as e:
        raise ProtocolError(f"campo fora do intervalo: {e}") from e
    return header + _encode_name(src, "src") + _encode_name(flow, "flow") + payload


def unpack_data(packet):
    """Descodifica um pacote de dados. Lança ProtocolError se for inválido."""
    if len(packet) < FIXED_HEADER.size:
        raise ProtocolError(f"pacote demasiado curto: {len(packet)} bytes")
    version, msg_type, ttl, seq, timestamp_ms = FIXED_HEADER.unpack_from(packet, 0)
    if version != config.PROTOCOL_VERSION:
        raise ProtocolError(f"versão não suportada: {version}")
    src, offset = _decode_name(packet, FIXED_HEADER.size, "src")
    flow, offset = _decode_name(packet, offset, "flow")
    return DataPacket(version, msg_type, ttl, seq, timestamp_ms, src, flow, packet[offset:])


def forward_packet(packet, new_src):
    """Prepara um pacote para o próximo salto: TTL - 1 e src = nó que reencaminha.

    O payload é copiado sem ser interpretado; seq e timestamp_ms mantêm-se (são do servidor).
    """
    p = unpack_data(packet)
    if p.ttl == 0:
        raise ProtocolError(f"TTL esgotado (fluxo {p.flow}, seq {p.seq})")
    return pack_data(new_src, p.flow, p.seq, p.timestamp_ms, p.ttl - 1, p.payload)
