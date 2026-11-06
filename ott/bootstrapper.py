"""Bootstrapper: lê a topologia de um ficheiro JSON e responde a REGISTER com a lista de vizinhos.

Uso: python3 -m ott.bootstrapper <ficheiro_topologia.json> [--bind <ip>]

O bootstrapper é apenas o ponto de encontro inicial: um nó novo não sabe onde estão os
outros e pergunta-lhe a quem se deve ligar. Depois de receber a resposta, o nó nunca
mais fala com ele. Por isso o bootstrapper:
- não guarda estado sobre os nós (não sabe quem está vivo nem quem já se registou);
- não calcula rotas, não deteta falhas e não reconstrói a rede.
Tudo isso é distribuído entre os nós. Cada pedido é tratado de forma independente:
lê REGISTER, responde NEIGHBORS (ou ERROR) e fecha a ligação.
"""

import argparse
import ipaddress
import json
import signal
import socket
import threading
import time

from ott import config
from ott.log import get_logger
from ott.protocol import control

# Valor do campo src nas mensagens enviadas pelo bootstrapper
BOOTSTRAPPER_ID = "bootstrapper"


class TopologyError(Exception):
    """Ficheiro de topologia inválido."""


def load_topology(path, logger):
    """Lê e valida o ficheiro de topologia. Devolve o dicionário nodes: id -> {"ip", "neighbors"}."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise TopologyError(f"não foi possível ler {path}: {e}") from e

    nodes = data.get("nodes") if isinstance(data, dict) else None
    if not isinstance(nodes, dict) or not nodes:
        raise TopologyError("o ficheiro tem de ter um objeto 'nodes' não vazio")

    for node_id, info in nodes.items():
        if not isinstance(info, dict) or "ip" not in info or "neighbors" not in info:
            raise TopologyError(f"{node_id}: faltam os campos 'ip' e/ou 'neighbors'")
        try:
            ipaddress.ip_address(info["ip"])
        except ValueError as e:
            raise TopologyError(f"{node_id}: IP inválido '{info['ip']}'") from e
        if not isinstance(info["neighbors"], list):
            raise TopologyError(f"{node_id}: 'neighbors' tem de ser uma lista")
        for neighbor_id in info["neighbors"]:
            if neighbor_id == node_id:
                raise TopologyError(f"{node_id}: um nó não pode ser vizinho de si próprio")
            if neighbor_id not in nodes:
                raise TopologyError(f"{node_id}: vizinho desconhecido '{neighbor_id}'")

    # Assimetria não é um erro (o HELLO cria a ligação nos dois sentidos), mas é provavelmente engano
    for node_id, info in nodes.items():
        for neighbor_id in info["neighbors"]:
            if node_id not in nodes[neighbor_id]["neighbors"]:
                logger.warning(f"vizinhança não simétrica: {node_id} -> {neighbor_id}, "
                               f"mas {neighbor_id} não tem {node_id}")
    return nodes


class Bootstrapper:
    def __init__(self, nodes, logger, bind=config.ANY_IP, port=config.BOOTSTRAP_PORT):
        self.nodes = nodes
        self.logger = logger
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((bind, port))
        self.sock.listen()
        # Porta real (útil quando port=0, nos testes)
        self.port = self.sock.getsockname()[1]
        self._stopping = False

    def serve_forever(self):
        self.logger.info(f"bootstrapper à escuta em {self.sock.getsockname()[0]}:{self.port}, "
                         f"{len(self.nodes)} nós na topologia")
        while True:
            try:
                conn, addr = self.sock.accept()
            except OSError as e:
                if self._stopping:
                    return
                # Erro temporário (ex.: sem descritores livres): desistir deixava os nós sem bootstrapper
                self.logger.warning(f"erro em accept(): {e}")
                time.sleep(config.ACCEPT_RETRY_DELAY)
                continue
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def stop(self):
        self._stopping = True
        try:
            # Em Linux, close() sozinho não acorda uma thread bloqueada em accept(); shutdown() acorda
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    def _handle(self, conn, addr):
        """Trata um pedido: REGISTER -> NEIGHBORS | ERROR, e fecha a ligação."""
        with conn:
            conn.settimeout(config.BOOTSTRAP_REQUEST_TIMEOUT)
            try:
                msg = control.recv_msg(conn)
                if msg is None:
                    self.logger.warning(f"{addr[0]} fechou a ligação sem enviar REGISTER")
                    return
                control.validate(msg)
                reply = self._reply_to(msg)
                control.send_msg(conn, reply)
            except (OSError, control.ProtocolError) as e:
                self.logger.warning(f"pedido de {addr[0]} falhou: {e}")
                return

        if reply["type"] == control.NEIGHBORS:
            names = ", ".join(n["id"] for n in reply["neighbors"]) or "(nenhum)"
            self.logger.info(f"REGISTER de {msg['src']} ({addr[0]}) -> NEIGHBORS [{names}]")
        else:
            self.logger.warning(f"{msg['type']} de {msg['src']} ({addr[0]}) -> ERROR {reply['reason']}")

    def _reply_to(self, msg):
        if msg["type"] != control.REGISTER:
            return control.make_msg(control.ERROR, BOOTSTRAPPER_ID, reason="expected REGISTER")
        node = self.nodes.get(msg["src"])
        if node is None:
            return control.make_msg(control.ERROR, BOOTSTRAPPER_ID, reason="unknown node")
        neighbors = [{"id": n, "ip": self.nodes[n]["ip"]} for n in node["neighbors"]]
        return control.make_msg(control.NEIGHBORS, BOOTSTRAPPER_ID, neighbors=neighbors)


def main():
    parser = argparse.ArgumentParser(description="Bootstrapper do overlay OTT")
    parser.add_argument("topology", help="ficheiro JSON com a topologia")
    parser.add_argument("--bind", default=config.ANY_IP, help="IP onde escutar (por omissão 0.0.0.0)")
    args = parser.parse_args()

    # Um processo lançado em background por um script (cmd &) começa com o SIGINT ignorado:
    # repor o comportamento normal (KeyboardInterrupt) para Ctrl+C e kill -INT funcionarem sempre.
    # SIGTERM (o sinal por omissão do kill) também termina de forma limpa.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    logger = get_logger(BOOTSTRAPPER_ID)
    try:
        nodes = load_topology(args.topology, logger)
    except TopologyError as e:
        logger.error(f"topologia inválida: {e}")
        raise SystemExit(1)

    bootstrapper = Bootstrapper(nodes, logger, bind=args.bind)
    try:
        bootstrapper.serve_forever()
    except KeyboardInterrupt:
        logger.info("bootstrapper terminado")
        bootstrapper.stop()


if __name__ == "__main__":
    main()
