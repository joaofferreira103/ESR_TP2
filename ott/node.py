"""Ponto de entrada do nó overlay (python3 -m ott.node): lê a linha de comando, regista-se no bootstrapper e arranca as threads.

Uso: python3 -m ott.node <ID> <ip_bootstrapper> [--bind <ip>] [--server --streams nome=ficheiro,...] [--client]

Comandos no terminal: "vizinhos" mostra o estado; "metricas" mostra as métricas por vizinho; "candidatos" mostra os candidatos de reparação; "sair" termina (Ctrl+C também).
"""

import argparse
import signal
import sys
import threading
import time

from ott import config
from ott.log import get_logger
from ott.metrics import ProbeService, metrics_text
from ott.neighbors import NeighborManager
from ott.protocol import control
from ott.repair import OverlayRepair, candidates_text
from ott.scheduler import Scheduler
from ott.state import NodeState
from ott.transport.base import TransportError
from ott.transport.tcp import TcpTransport


def create_transport(kind, bind_ip, on_message, on_closed, logger):
    """Fábrica: o único sítio que escolhe a implementação do transporte de controlo."""
    if kind == "tcp":
        return TcpTransport(on_message, on_closed, logger, bind_ip)
    raise ValueError(f"transporte de controlo desconhecido: {kind}")


class Node:
    def __init__(self, node_id, bootstrapper_ip, bind_ip):
        self.node_id = node_id
        self.bootstrapper_ip = bootstrapper_ip
        self.bind_ip = bind_ip
        self.logger = get_logger(node_id)
        self.state = NodeState(node_id)
        self.neighbors = NeighborManager(
            self.state, self.logger,
            lambda on_message, on_closed: create_transport(config.CONTROL_TRANSPORT, bind_ip,
                                                           on_message, on_closed, self.logger))
        self.scheduler = Scheduler(self.logger)
        self.probes = ProbeService(self.state, self.neighbors, self.scheduler, self.logger, bind_ip)
        self.repair = OverlayRepair(self.state, self.neighbors, self.scheduler, self.logger,
                                    self._rebootstrap, bind_ip)
        self.stop_event = threading.Event()

    def register(self):
        """Pede a lista de vizinhos ao bootstrapper, tentando de novo até ele responder.

        Devolve {id: ip}. Termina o programa se o bootstrapper responder ERROR.
        """
        while True:
            try:
                reply = self._request_neighbors()
            except TransportError as e:
                self.logger.warning(f"bootstrapper {self.bootstrapper_ip} não respondeu ({e}); "
                                    f"nova tentativa em {config.BOOTSTRAP_RETRY_INTERVAL} s")
                time.sleep(config.BOOTSTRAP_RETRY_INTERVAL)
                continue

            if reply["type"] == control.ERROR:
                self.logger.error(f"o bootstrapper recusou o registo de {self.node_id}: {reply.get('reason')}")
                sys.exit(1)
            if reply["type"] != control.NEIGHBORS:
                self.logger.error(f"resposta inesperada do bootstrapper: {reply['type']}")
                sys.exit(1)
            return {n["id"]: n["ip"] for n in reply["neighbors"]}

    def _request_neighbors(self):
        """Um pedido REGISTER ao bootstrapper. Devolve a resposta; lança TransportError se falhar."""
        # O bootstrapper fala sempre TCP. O canal nunca é arrancado, por isso não há callbacks
        transport = create_transport("tcp", self.bind_ip, None, None, self.logger)
        channel, reply = transport.connect(self.bootstrapper_ip, config.BOOTSTRAP_PORT,
                                           control.make_msg(control.REGISTER, self.node_id),
                                           config.BOOTSTRAP_REQUEST_TIMEOUT)
        channel.close()
        return reply

    def _rebootstrap(self):
        """Último recurso da reparação: uma nova tentativa de arranque. Se falhar, a reparação tenta mais tarde."""
        try:
            reply = self._request_neighbors()
        except TransportError as e:
            self.logger.warning(f"bootstrapper {self.bootstrapper_ip} não respondeu ({e})")
            return
        if reply["type"] != control.NEIGHBORS:
            self.logger.warning(f"resposta inesperada do bootstrapper: {reply['type']}")
            return
        static = {n["id"]: n["ip"] for n in reply["neighbors"]}
        with self.state.lock:
            self.state.static_neighbors = static
        for neighbor_id, ip in static.items():
            self.neighbors.connect_async(neighbor_id, ip)

    def start(self):
        # 1-2. Registo no bootstrapper; a partir daqui nunca mais é contactado
        static = self.register()
        with self.state.lock:
            self.state.static_neighbors = static
        self.logger.info(f"vizinhos estáticos: {', '.join(f'{i} ({ip})' for i, ip in static.items()) or '(nenhum)'}")

        # 3. Probes e listener de controlo (probes primeiro: um vizinho novo tem logo quem lhe responda)
        self.probes.start()
        self.repair.start()
        self.neighbors.start()

        # 4. Primeira tentativa de ligação a todos os vizinhos estáticos
        for neighbor_id, ip in static.items():
            self.logger.info(f"a ligar a {neighbor_id} ({ip})")
            self.neighbors.connect_async(neighbor_id, ip)

        # 5. Tarefas periódicas
        self.scheduler.add_periodic("religar", config.RECONNECT_INTERVAL, self.neighbors.reconnect_static)
        self.scheduler.add_periodic("estado", config.STATE_DUMP_INTERVAL, self._dump_state)
        self.scheduler.start()

    def _dump_state(self):
        self.logger.info(self.state.snapshot())
        self.logger.info(metrics_text(self.state))
        self.logger.info(candidates_text(self.state))

    def stop(self):
        self.logger.info("a terminar")
        self.repair.stop()
        self.scheduler.stop()
        self.neighbors.stop()
        self.probes.stop()
        self.logger.info("terminado")

    def command_loop(self):
        """Interface no terminal (thread própria)."""
        for line in sys.stdin:
            command = line.strip()
            if command == "vizinhos":
                print(self.state.snapshot(), flush=True)
            elif command == "metricas":
                print(metrics_text(self.state), flush=True)
            elif command == "candidatos":
                print(candidates_text(self.state), flush=True)
            elif command == "sair":
                self.stop_event.set()
                return
            elif command:
                print("comandos: vizinhos | metricas | candidatos | sair", flush=True)
        # stdin fechado (ex.: nó lançado em segundo plano no CORE): o nó continua a correr

    def run(self):
        try:
            self.start()
            threading.Thread(target=self.command_loop, name="stdin", daemon=True).start()
            self.stop_event.wait()
        except KeyboardInterrupt:
            pass  # Ctrl+C a qualquer momento (também durante o registo) termina de forma limpa
        finally:
            self.stop()


def main():
    parser = argparse.ArgumentParser(description="Nó do overlay OTT")
    parser.add_argument("id", help="ID do nó (ex.: O1)")
    parser.add_argument("bootstrapper", help="IP do bootstrapper")
    parser.add_argument("--bind", default=config.ANY_IP, help="IP local a usar (por omissão 0.0.0.0)")
    parser.add_argument("--server", action="store_true", help="papel servidor (Fase 5)")
    parser.add_argument("--streams", default="", help="streams do servidor: nome=ficheiro,... (Fase 5)")
    parser.add_argument("--client", action="store_true", help="papel cliente (Fase 5)")
    args = parser.parse_args()

    # Um processo lançado em background por um script (cmd &) começa com o SIGINT ignorado:
    # repor o comportamento normal (KeyboardInterrupt) para Ctrl+C e kill -INT terminarem de
    # forma limpa. SIGTERM fica com o comportamento por omissão (morte imediata, sem LEAVE),
    # que é o que se usa nos testes para simular a falha de um nó.
    signal.signal(signal.SIGINT, signal.default_int_handler)

    node = Node(args.id, args.bootstrapper, args.bind)
    if args.server or args.streams:
        node.logger.info("opções --server/--streams aceites, mas o papel servidor fica para a Fase 5")
    if args.client:
        node.logger.info("opção --client aceite, mas o papel cliente fica para a Fase 5")
    node.run()


if __name__ == "__main__":
    main()
