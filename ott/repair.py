"""Autorreparação do overlay: gossip de vizinhos e criação de uma aresta nova quando um nó fica isolado.

Regra do docente: só se cria uma aresta nova quando o nó fica sem nenhuma ligação. Perder um
vizinho, tendo outros, não muda nada.

Os candidatos (vizinhos dos vizinhos) são recolhidos ANTES de serem precisos e guardados mesmo
depois de morrer o vizinho que os indicou: quando o nó fica isolado, já não tem a quem perguntar.
A lista é enviada periodicamente e também sempre que um vizinho fica "up".

Cada entrada da NEIGHBOR_LIST leva o grau do vizinho ({id, ip, degree}), que o emissor conhece pela
NEIGHBOR_LIST que esse vizinho lhe enviou. Na reparação, um candidato com grau <= 1 indicado só por
vizinhos que já morreram fica para o fim: o seu único vizinho era provavelmente o nó que falhou, e
ligar-se a ele formava uma ilha desligada do resto do overlay (erro 1).

Concorrência: os candidatos vivem em NodeState.overlay_candidates e só se acedem com o lock global.
A reparação corre numa thread própria, porque medir RTTs e fazer HELLO bloqueia.
"""

import ipaddress
import threading
import time

from ott import config
from ott.metrics import measure_rtt
from ott.protocol import control

PENDING_POLL = 0.05     # s: intervalo entre verificações enquanto se espera por uma ligação pendente


def _degree(entry):
    """Grau indicado numa entrada da NEIGHBOR_LIST, ou None se não vier ou for inválido."""
    degree = entry.get("degree")
    if isinstance(degree, int) and not isinstance(degree, bool) and degree >= 0:
        return degree
    return None


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
            degree = "?" if c["degree"] is None else c["degree"]
            lines.append(f"  {candidate_id} {c['ip']} grau {degree} indicado por {', '.join(sorted(c['via']))} "
                         f"há {now - c['last_seen']:.0f}s")
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
        with self.state.lock:
            listing = [{"id": n.id, "ip": n.ip, "degree": self.state.neighbor_degrees.get(n.id)} for n in up]
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
            # A lista do vizinho são os vizinhos dele: o seu tamanho é o grau dele
            degree_changed = self.state.neighbor_degrees.get(neighbor_id) != len(entries)
            self.state.neighbor_degrees[neighbor_id] = len(entries)
            for entry in entries:
                if not _valid_entry(entry):
                    self.logger.warning(f"entrada inválida na NEIGHBOR_LIST de {neighbor_id}: {entry}")
                elif entry["id"] != self.state.node_id:
                    candidate = self.state.overlay_candidates.setdefault(entry["id"], {"via": set()})
                    candidate.update(ip=entry["ip"], degree=_degree(entry), last_seen=now)
                    candidate["via"].add(neighbor_id)
        if degree_changed:
            # A minha lista leva o grau deste vizinho: reenviá-la já, para os outros não ficarem com um
            # grau antigo até ao próximo GOSSIP_INTERVAL. Termina sozinho: reenviar a minha lista não
            # muda o meu grau, por isso não provoca novos reenvios
            self.send_gossip()

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
        self.release_dynamic_edges()

    def release_dynamic_edges(self):
        """Fecha (com LEAVE) as arestas dinâmicas criadas por este nó quando já tem um vizinho
        estático "up": deixaram de ser precisas, e assim a topologia volta à do ficheiro."""
        with self.state.lock:
            up = [n for n in self.state.neighbors.values() if n.status == "up"]
            if not any(n.static for n in up):
                return
            mine = [n for n in up if n.dynamic and n.initiated_by_me]
        for neighbor in mine:
            self.neighbors.leave_neighbor(neighbor, "aresta dinâmica já não é precisa: vizinho estático de volta")

    def _on_neighbor_down(self, neighbor_id, reason):
        with self.state.lock:
            self.state.neighbor_degrees.pop(neighbor_id, None)
        remaining = self.state.count_up()
        if remaining > 0:
            self.logger.info(f"perdi {neighbor_id}; vizinhos restantes: {remaining}; sem aresta nova")
            return
        self.start_repair()

    def periodic_check(self):
        """Remove candidatos expirados, fecha arestas dinâmicas que já não são precisas e volta a
        tentar se a última reparação não resultou."""
        self.expire_candidates()
        self.release_dynamic_edges()
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
                self._log_cancelled()
                return
            self.logger.info(f"reparação: HELLO a {candidate_id} ({ip}), RTT {rtt * 1000:.2f} ms")
            with self.state.lock:
                # Voltar a ligar a um vizinho original não é uma aresta nova
                dynamic = candidate_id not in self.state.static_neighbors
            if self.neighbors.connect_to(candidate_id, ip, dynamic=dynamic):
                # Liga-se a um só: depois disto já não está isolado
                self.logger.info(f"reparação concluída: ligado a {candidate_id}")
                return
            self.logger.info(f"reparação: {candidate_id} não aceitou, a tentar o seguinte")
        # Um candidato que se ligou a mim ao mesmo tempo fica pendente à espera do HELLO_CONFIRM e faz
        # falhar a minha tentativa a ele: esperar que essa ligação se resolva antes de desistir
        self._wait_for_pending_incoming()
        if self._no_longer_needed():
            self._log_cancelled()
            return
        self.logger.warning("reparação: nenhum candidato aceitou, a voltar ao bootstrapper")
        self.on_exhausted()

    def _wait_for_pending_incoming(self):
        """Espera (no máximo HELLO_TIMEOUT) que as ligações de entrada pendentes cheguem a "up" ou desistam."""
        deadline = time.monotonic() + config.HELLO_TIMEOUT
        while time.monotonic() < deadline:
            with self.state.lock:
                pending = any(n.status != "up" and not n.initiated_by_me
                              for n in self.state.neighbors.values())
            if not pending:
                return
            time.sleep(PENDING_POLL)

    def _log_cancelled(self):
        with self.state.lock:
            stopping = self._stopping
            returned = sorted(n.id for n in self.state.neighbors.values() if n.status == "up")
        if stopping:
            self.logger.info("reparação cancelada: o nó está a terminar")
        else:
            self.logger.info(f"reparação cancelada: {', '.join(returned)} voltou")

    def _rank_candidates(self):
        """Devolve [(id, ip, rtt)] dos candidatos que respondem, do melhor para o pior.

        Os candidatos suspeitos (grau <= 1, indicados só por vizinhos que já morreram) ficam no fim,
        ordenados pelo RTT entre si: só são usados se nenhum outro aceitar.
        """
        with self.state.lock:
            up = {n.id for n in self.state.neighbors.values() if n.status == "up"}
            candidates = {}
            for i, c in self.state.overlay_candidates.items():
                if i not in self.state.neighbors:
                    suspect = c["degree"] is not None and c["degree"] <= 1 and not (c["via"] & up)
                    candidates[i] = (c["ip"], suspect)
        self.logger.info(f"reparação: {len(candidates)} candidatos a medir "
                         f"({', '.join(sorted(candidates)) or 'nenhum'})")
        ranked = []
        for candidate_id, (ip, suspect) in candidates.items():
            rtt = self.measure(ip, config.REPAIR_PROBES, config.REPAIR_TIMEOUT, node_id=self.state.node_id,
                               bind_ip=self.bind_ip, port=self.probe_port)
            if rtt is None:
                self.logger.info(f"reparação: {candidate_id} não respondeu aos probes")
                continue
            if suspect:
                self.logger.info(f"reparação: {candidate_id} fica para o fim (grau <= 1, indicado só por "
                                 f"vizinhos que morreram: ligar-se a ele podia formar uma ilha)")
            ranked.append((suspect, rtt, candidate_id, ip))
        return [(candidate_id, ip, rtt) for suspect, rtt, candidate_id, ip in sorted(ranked)]

    def _no_longer_needed(self):
        """Entretanto voltou um vizinho (ex.: um original que religou), ou o nó está a terminar."""
        with self.state.lock:
            return self._stopping or self.state.count_up() > 0
