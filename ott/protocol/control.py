"""Protocolo de controlo: enquadramento (4 bytes de tamanho + JSON), construção e validação de mensagens.

TCP (controlo): cada mensagem é [tamanho u32 big-endian][JSON UTF-8].
UDP (probes): cada datagrama é um JSON, sem prefixo (o datagrama já delimita a mensagem).
"""

import json
import struct

from ott import config

# --- Tipos de mensagem ---
# Fase 1: arranque e vizinhos
REGISTER = "REGISTER"
NEIGHBORS = "NEIGHBORS"
ERROR = "ERROR"
HELLO = "HELLO"
HELLO_ACK = "HELLO_ACK"
HELLO_REJECT = "HELLO_REJECT"
LEAVE = "LEAVE"
# Fase 2: probes (UDP)
PROBE = "PROBE"
PROBE_REPLY = "PROBE_REPLY"
PROBE_PAIR = "PROBE_PAIR"
# Fase 3: rotas
ANNOUNCE = "ANNOUNCE"
PING = "PING"
# Fase 4: ativação
ACTIVATE = "ACTIVATE"
DEACTIVATE = "DEACTIVATE"
# Fase 2: gossip de vizinhos, para a autorreparação
NEIGHBOR_LIST = "NEIGHBOR_LIST"

# Prefixo de tamanho: inteiro sem sinal de 4 bytes, big-endian
LENGTH_PREFIX = struct.Struct("!I")


class ProtocolError(Exception):
    """Mensagem mal formada: tamanho excessivo, JSON inválido, campos em falta ou versão errada."""


def make_msg(msg_type, src, **fields):
    """Cria uma mensagem com os campos comuns (v, type, src) e os campos específicos."""
    msg = {"v": config.PROTOCOL_VERSION, "type": msg_type, "src": src}
    msg.update(fields)
    return msg


def validate(msg):
    """Confirma que a mensagem tem os campos comuns e a versão certa."""
    if not isinstance(msg, dict):
        raise ProtocolError("a mensagem não é um objeto JSON")
    for field in ("v", "type", "src"):
        if field not in msg:
            raise ProtocolError(f"campo em falta: {field}")
    if msg["v"] != config.PROTOCOL_VERSION:
        raise ProtocolError(f"versão não suportada: {msg['v']}")
    if not isinstance(msg["type"], str) or not isinstance(msg["src"], str):
        raise ProtocolError("os campos type e src têm de ser texto")


def _encode_json(msg_dict):
    # ensure_ascii=False: acentos ficam legíveis no Wireshark e nos logs
    return json.dumps(msg_dict, ensure_ascii=False).encode("utf-8")


def _decode_json(data):
    try:
        msg = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError(f"JSON inválido: {e}") from e
    if not isinstance(msg, dict):
        raise ProtocolError("a mensagem não é um objeto JSON")
    return msg


def send_msg(sock, msg_dict):
    """Envia uma mensagem de controlo por TCP (prefixo de tamanho + JSON).

    Num socket de vizinho, só a thread escritora desse vizinho deve chamar esta
    função (ver decisoesTP.txt, Concorrência): duas threads a escrever no mesmo
    socket podiam intercalar os bytes de mensagens diferentes.
    """
    body = _encode_json(msg_dict)
    if len(body) > config.MAX_CONTROL_MSG:
        raise ProtocolError(f"mensagem demasiado grande: {len(body)} bytes")
    sock.sendall(LENGTH_PREFIX.pack(len(body)) + body)


def recv_exact(sock, n):
    """Lê exatamente n bytes. O TCP pode entregá-los em vários pedaços.

    Devolve None se a ligação fechar antes de chegarem os n bytes.
    """
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_msg(sock):
    """Recebe uma mensagem de controlo por TCP. Devolve o dicionário, ou None se a ligação fechou."""
    header = recv_exact(sock, LENGTH_PREFIX.size)
    if header is None:
        return None
    (length,) = LENGTH_PREFIX.unpack(header)
    # Verificar antes de ler: um tamanho absurdo não pode levar a reservar memória sem limite
    if length > config.MAX_CONTROL_MSG:
        raise ProtocolError(f"mensagem demasiado grande: {length} bytes")
    body = recv_exact(sock, length)
    if body is None:
        return None
    return _decode_json(body)


def encode_udp(msg_dict):
    """Codifica uma mensagem para um datagrama UDP (probes): JSON sem prefixo."""
    return _encode_json(msg_dict)


def decode_udp(data):
    """Descodifica um datagrama UDP (probes). Lança ProtocolError se o JSON for inválido."""
    return _decode_json(data)
