"""Autorreparação do overlay: gossip de vizinhos e criação de uma aresta nova quando um nó fica isolado.

Regra do docente: só se cria uma aresta nova quando o nó fica sem nenhuma ligação. Perder um
vizinho, tendo outros, não muda nada.

Os candidatos (vizinhos dos vizinhos) são recolhidos ANTES de serem precisos e guardados mesmo
depois de morrer o vizinho que os indicou: quando o nó fica isolado, já não tem a quem perguntar.
A lista é enviada periodicamente e também sempre que um vizinho fica "up".

Concorrência: os candidatos vivem em NodeState.overlay_candidates e só se acedem com o lock global.
A reparação corre numa thread própria, porque medir RTTs e fazer HELLO bloqueia.
"""

import ipaddress
import threading
import time

from ott import config
from ott.metrics import measure_rtt
from ott.protocol import control


def _valid_entry(entry):
    """Uma entrada da NEIGHBOR_LIST tem um id e um IP válidos (a mensagem vem de outro nó)."""
    if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
        return False
    try:
        ipaddress.ip_address(entry.get("ip"))
    except ValueError:
        return False
    return True


def candidates_text(state):
    """Texto com os candidatos de reparação, para o comando "candidatos" e para os logs."""
    now = time.monotonic()
    with state.lock:
        items = sorted(state.overlay_candidates.items())
        lines = [f"Candidatos de {state.node_id}: {len(items)}"]
        for candidate_id, c in items:
            lines.append(f"  {candidate_id} {c['ip']} indicado por {c['via']} há {now - c['last_seen']:.0f}s")
    return "\n".join(lines)


class OverlayRepair:
    def __init__(self, state, neighbors, scheduler, logger, on_exhausted, bind_ip=config.ANY_IP,
                 probe_port=config.PROBE_PORT, measure=measure_rtt):
        """on_exhausted() é chamada (na thread de reparação) se nenhum candidato aceitar a ligação."""
        self.state = state
        self.neighbors = neighbors      # NeighborManager
        self.scheduler = scheduler
        self.logger = logger
        self.on_exhausted = on_exhausted
        self.bind_ip = bind_ip
        self.probe_port = probe_port
        self.measure = measure
        self._had_neighbors = False     # um nó que nunca teve vizinhos está a arrancar, não isolado
        self._repairing = False
        self._stopping = False
        neighbors.register_handler(control.NEIGHBOR_LIST, self._on_neighbor_list)
        neighbors.add_up_listener(self._on_neighbor_up)
        neighbors.add_down_listener(self._on_neighbor_down)

    def start(self):
        self.scheduler.add_periodic("gossip", config.GOSSIP_INTERVAL, self.send_gossip)
        self.scheduler.add_periodic("candidatos", config.REPAIR_RETRY_INTERVAL, self.periodic_check)

    def stop(self):
        with self.state.lock:
            self._stopping = True

    # --- Monitorização (gossip) ---

    def send_gossip(self):
        """Envia a cada vizinho a lista dos vizinhos deste nó."""
        up = self.state.up_neighbors()
        listing = [{"id": n.id, "ip": n.ip} for n in up]
        msg = control.make_msg(control.NEIGHBOR_LIST, self.state.node_id, neighbors=listing)
        for neighbor in up:
            self.neighbors.send_to(neighbor.id, msg)

    def _on_neighbor_list(self, neighbor_id, msg):
        entries = msg.get("neighbors")
        if not isinstance(entries, list):
            self.logger.warning(f"NEIGHBOR_LIST inválida de {neighbor_id}")
            return
        now = time.monotonic()
        with self.state.lock:
            for entry in entries:
                if not _valid_entry(entry):
                    self.logger.warning(f"entrada inválida na NEIGHBOR_LIST de {neighbor_id}: {entry}")
                elif entry["id"] != self.state.node_id:
                    self.state.overlay_candidates[entry["id"]] = {
                        "ip": entry["ip"], "via": neighbor_id, "last_seen": now}

    def expire_candidates(self, now=None):
        now = time.monotonic() if now is None else now
        with self.state.lock:
            candidates = self.state.overlay_candidates
            for candidate_id in [i for i, c in candidates.items()
                                 if now - c["last_seen"] > config.CANDIDATE_EXPIRY]:
                del candidates[candidate_id]

    # --- Quando reparar ---

    def _on_neighbor_up(self, neighbor_id):
        with self.state.lock:
            self._had_neighbors = True
        # Sem esperar pelo GOSSIP_INTERVAL: o vizinho novo fica logo com candidatos, e os outros
        # recebem a lista atualizada, de que ele já faz parte. send_gossip só coloca nas filas
        self.send_gossip()

    def _on_neighbor_down(self, neighbor_id, reason):
        remaining = self.state.count_up()
        if remaining > 0:
            self.logger.info(f"perdi {neighbor_id}; vizinhos restantes: {remaining}; sem aresta nova")
            return
        self.start_repair()

    def periodic_check(self):
        """Remove candidatos expirados e volta a tentar se a última reparação não resultou."""
        self.expire_candidates()
        self.start_repair()

    def start_repair(self):
        """Arranca a reparação se o nó estiver isolado e não houver outra a decorrer. Não bloqueia."""
        with self.state.lock:
            if (self._stopping or self._repairing or not self._had_neighbors
                    or self.state.count_up() > 0):
                return
            self._repairing = True
        threading.Thread(target=self._run_repair, name="reparacao", daemon=True).start()

    # --- Reparação ---

    def _run_repair(self):
        try:
            self.repair()
        except Exception:
            self.logger.exception("erro na reparação")
        finally:
            with self.state.lock:
                self._repairing = False

    def repair(self):
        """Mede os candidatos e liga-se ao melhor que aceitar. Bloqueia."""
        self.logger.warning("nó isolado: a iniciar a reparação")
        for candidate_id, ip, rtt in self._rank_candidates():
            if self._no_longer_needed():
                return
            self.logger.info(f"reparação: HELLO a {candidate_id} ({ip}), RTT {rtt * 1000:.2f} ms")
            if self.neighbors.connect_to(candidate_id, ip):
                # Liga-se a um só: depois disto já não está isolado
                self.logger.info(f"reparação concluída: ligado a {candidate_id}")
                return
            self.logger.info(f"reparação: {candidate_id} não aceitou, a tentar o seguinte")
        if not self._no_longer_needed():
            self.logger.warning("reparação: nenhum candidato aceitou, a voltar ao bootstrapper")
            self.on_exhausted()

    def _rank_candidates(self):
        """Devolve [(id, ip, rtt)] dos candidatos que respondem, do melhor para o pior."""
        with self.state.lock:
            candidates = {i: c["ip"] for i, c in self.state.overlay_candidates.items()
                          if i not in self.state.neighbors}
        self.logger.info(f"reparação: {len(candidates)} candidatos a medir "
                         f"({', '.join(sorted(candidates)) or 'nenhum'})")
        ranked = []
        for candidate_id, ip in candidates.items():
            rtt = self.measure(ip, config.REPAIR_PROBES, config.REPAIR_TIMEOUT, node_id=self.state.node_id,
                               bind_ip=self.bind_ip, port=self.probe_port)
            if rtt is None:
                self.logger.info(f"reparação: {candidate_id} não respondeu aos probes")
            else:
                ranked.append((rtt, candidate_id, ip))
        return [(candidate_id, ip, rtt) for rtt, candidate_id, ip in sorted(ranked)]

    def _no_longer_needed(self):
        """Entretanto voltou um vizinho (ex.: um original que religou), ou o nó está a terminar."""
        with self.state.lock:
            return self._stopping or self.state.count_up() > 0
