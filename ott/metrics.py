"""Métricas por link: probes UDP, RTT (SRTT e RTTVAR), perda, capacidade (packet pair) e deteção de falhas (heartbeat).

Medir o RTT com o relógio do próprio nó evita a sincronização de relógios: o instante de envio e
o de receção são lidos no mesmo nó, por isso só interessa a diferença entre eles. O outro nó limita-se
a devolver a mensagem e nunca lê o seu relógio, logo a diferença entre os relógios dos dois não
entra na conta. Usa-se time.monotonic() porque, ao contrário de time.time(), nunca anda para trás
(por exemplo com um acerto de NTP), o que daria RTTs errados ou negativos.

Unidades: internamente em segundos; snapshot() e os textos para os logs em milissegundos.
Concorrência: os LinkMetrics vivem em NodeState.link_metrics e só se acedem com o lock global.
O envio de datagramas faz-se sempre sem o lock.
"""

import collections
import socket
import statistics
import threading
import time

from ott import config
from ott.cost import BITS_PER_MBIT, MS_PER_SECOND, link_cost
from ott.protocol import control

# Campo src dos probes de measure_rtt quando quem chama não indica o ID do nó
MEASURE_SRC = "measure"
DEAD_REASON = "sem resposta aos probes"
PAD_CHAR = "x"                  # 1 byte em UTF-8: o tamanho do pad é o número de caracteres
BITS_PER_BYTE = 8


class LinkMetrics:
    """Métricas de um link. Classe pura: os instantes vêm de fora, sem sockets nem threads."""

    def __init__(self, now=None):
        self.created_at = time.monotonic() if now is None else now
        self.srtt = None
        self.rttvar = None
        self.samples = 0                # número de respostas aceites
        self.last_reply_time = None
        # Últimos LOSS_WINDOW probes, do mais antigo para o mais recente: seq -> [instante de envio, respondido]
        self._probes = {}
        self._pair_start = None         # (pair_id, instante de chegada do idx 0)
        self._capacities = collections.deque(maxlen=config.PAIR_HISTORY)    # estimativas em bits/s
        self._data = collections.deque()    # (instante, bytes) dos dados recebidos, nos últimos BW_WINDOW s

    def on_probe_sent(self, seq, ts):
        self._probes[seq] = [ts, False]
        if len(self._probes) > config.LOSS_WINDOW:
            del self._probes[next(iter(self._probes))]  # os dicionários mantêm a ordem de inserção

    def on_reply(self, seq, now):
        """Regista a resposta ao probe seq. Duplicados e respostas fora de tempo são ignorados.

        O RTT usa o instante de envio guardado aqui, e não o ts da resposta: assim um datagrama
        estranho nunca consegue introduzir um RTT falso.
        """
        probe = self._probes.get(seq)
        if probe is None or probe[1]:
            return
        rtt = now - probe[0]
        if rtt > config.DEAD_TIMEOUT:
            return  # já contou como perdido
        probe[1] = True
        self.samples += 1
        self.last_reply_time = now
        if self.srtt is None:
            self.srtt = rtt
            self.rttvar = rtt / 2
        else:
            # Como no TCP: o RTTVAR atualiza-se primeiro, porque compara com o SRTT anterior
            self.rttvar = (1 - config.RTTVAR_BETA) * self.rttvar + config.RTTVAR_BETA * abs(rtt - self.srtt)
            self.srtt = (1 - config.RTT_ALPHA) * self.srtt + config.RTT_ALPHA * rtt

    def loss(self, now=None):
        """Fração de probes perdidos na janela. Os que ainda podem ter resposta não contam."""
        now = time.monotonic() if now is None else now
        answered = lost = 0
        for ts, replied in self._probes.values():
            if replied:
                answered += 1
            elif now - ts > config.DEAD_TIMEOUT:
                lost += 1
        total = answered + lost
        return lost / total if total else 0.0

    def is_ready(self):
        return self.samples >= config.MIN_SAMPLES

    def silent_for(self, now):
        """Tempo sem respostas. Um link novo conta desde a criação, para ter tempo de responder."""
        return now - (self.created_at if self.last_reply_time is None else self.last_reply_time)

    def on_pair_packet(self, pair_id, idx, now):
        """Regista a chegada de um PROBE_PAIR enviado pelo vizinho deste link.

        O valor fica no link do vizinho que ENVIOU o par porque mede o sentido vizinho -> eu, que é
        o sentido em que os dados chegam, e é este o link cujo custo o nó soma aos anúncios que recebe.
        O link mais lento do caminho espaça os dois pacotes: entre as duas chegadas passa o tempo de
        transmitir um pacote nesse link.

        Um par é descartado se faltar um dos pacotes ou se chegarem trocados: o idx 1 sem o idx 0 do
        mesmo pair_id não conta, e um idx 0 sem par é substituído pelo do par seguinte.
        """
        if idx == 0:
            self._pair_start = (pair_id, now)
            return
        start = self._pair_start
        self._pair_start = None
        if start is None or start[0] != pair_id:
            return
        interval = now - start[1]
        if interval > 0:
            self._capacities.append(config.PAIR_SIZE * BITS_PER_BYTE / interval)

    def capacity(self):
        """Capacidade estimada em bits/s, ou None se ainda não for conhecida.

        O método é ruidoso (qualquer atraso num dos pacotes muda muito o intervalo): a mediana das
        últimas estimativas ignora os valores extremos, ao contrário da média.
        """
        return statistics.median(self._capacities) if self._capacities else None

    def record_data_bytes(self, n, now):
        """Gancho para a Fase 4: regista n bytes de dados recebidos deste vizinho."""
        self._data.append((now, n))
        self._trim_data(now)

    def throughput(self, now=None):
        """Débito recebido nos últimos BW_WINDOW s, em bits/s."""
        now = time.monotonic() if now is None else now
        self._trim_data(now)
        return sum(n for _, n in self._data) * BITS_PER_BYTE / config.BW_WINDOW

    def _trim_data(self, now):
        while self._data and now - self._data[0][0] > config.BW_WINDOW:
            self._data.popleft()

    def snapshot(self, now=None):
        """srtt e rttvar em ms (None sem amostras), loss como fração, samples, capacity em bits/s (ou None)."""
        return {
            "srtt": None if self.srtt is None else self.srtt * MS_PER_SECOND,
            "rttvar": None if self.rttvar is None else self.rttvar * MS_PER_SECOND,
            "loss": self.loss(now),
            "samples": self.samples,
            "capacity": self.capacity(),
        }


def metrics_text(state):
    """Texto com as métricas de cada vizinho, para o comando "metricas" e para os logs."""
    def ms(value):
        return "-" if value is None else f"{value:.2f} ms"

    def mbit(value):
        return "-" if value is None else f"{value / BITS_PER_MBIT:.1f} Mbit/s"

    with state.lock:
        items = sorted((nid, m.snapshot(), link_cost(m)) for nid, m in state.link_metrics.items())
    lines = [f"Métricas de {state.node_id}: {len(items)} vizinhos"]
    for nid, s, cost in items:
        lines.append(f"  {nid} srtt={ms(s['srtt'])} rttvar={ms(s['rttvar'])} "
                     f"perda={s['loss'] * 100:.1f}% amostras={s['samples']} "
                     f"capacidade={mbit(s['capacity'])} custo={cost:.2f} ms")
    return "\n".join(lines)


def _parse_probe(data):
    """Devolve a mensagem de um datagrama de probe, ou None se for inválido."""
    try:
        msg = control.decode_udp(data)
        control.validate(msg)
    except control.ProtocolError:
        return None
    if msg["type"] == control.PROBE_PAIR:
        valid = isinstance(msg.get("pair_id"), int) and msg.get("idx") in (0, 1)
    else:
        valid = isinstance(msg.get("seq"), int)
    return msg if valid else None


def measure_rtt(ip, n, timeout, node_id=MEASURE_SRC, bind_ip=config.ANY_IP, port=config.PROBE_PORT):
    """Envia n probes a um nó qualquer e devolve o RTT médio em segundos, ou None sem respostas.

    Bloqueia até timeout s (no total): não chamar a partir do scheduler nem com o lock global.
    Usa um socket próprio, para não se misturar com os probes do ProbeService. O socket não fica
    "ligado" ao ip porque um nó com várias interfaces pode responder por um IP diferente.
    """
    sent = {}
    rtts = []
    deadline = time.monotonic() + timeout
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        if bind_ip != config.ANY_IP:
            try:
                sock.bind((bind_ip, 0))
            except OSError:
                return None
        for seq in range(n):
            try:
                ts = time.monotonic()
                sent[seq] = ts
                sock.sendto(control.encode_udp(control.make_msg(control.PROBE, node_id, seq=seq, ts=ts)), (ip, port))
            except OSError:
                sent.pop(seq, None)  # destino inalcançável: conta como probe sem resposta
        while sent:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            try:
                data, _ = sock.recvfrom(config.PROBE_MAX_SIZE)
            except OSError:
                break
            now = time.monotonic()
            msg = _parse_probe(data)
            if msg is not None and msg["type"] == control.PROBE_REPLY and msg["seq"] in sent:
                rtts.append(now - sent.pop(msg["seq"]))
    return sum(rtts) / len(rtts) if rtts else None


class ProbeService:
    """Envia e responde a probes, atualiza os LinkMetrics e deteta os vizinhos que deixam de responder."""

    def __init__(self, state, neighbors, scheduler, logger, bind_ip=config.ANY_IP, port=config.PROBE_PORT):
        self.state = state
        self.neighbors = neighbors      # NeighborManager
        self.scheduler = scheduler
        self.logger = logger
        self.bind_ip = bind_ip
        self.port = port
        self._sock = None
        self._thread = None
        self._stop = threading.Event()
        # Um contador único para todos os vizinhos: uma resposta atrasada de uma ligação
        # anterior nunca coincide com um probe da ligação nova ao mesmo vizinho
        self._next_seq = 0
        self._next_pair_id = 0
        neighbors.add_up_listener(self._on_neighbor_up)
        neighbors.add_down_listener(self._on_neighbor_down)

    def start(self):
        """Abre o socket e arranca a receção e as tarefas periódicas. Chamar antes de haver vizinhos."""
        # Sem SO_REUSEADDR: em UDP deixaria dois nós partilharem a porta sem dar erro
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self._sock.bind((self.bind_ip, self.port))
        except OSError:
            self._sock.close()
            raise
        # close() não acorda um recvfrom() bloqueado: o timeout permite à thread ver o pedido de paragem
        self._sock.settimeout(config.PROBE_RECV_TIMEOUT)
        self._thread = threading.Thread(target=self._recv_loop, name="probes", daemon=True)
        self._thread.start()
        self.scheduler.add_periodic("probes", config.PROBE_INTERVAL, self.send_probes)
        self.scheduler.add_periodic("verificar-vizinhos", config.PROBE_INTERVAL, self.check_dead)
        self.scheduler.add_periodic("packet-pair", config.PAIR_INTERVAL, self.send_pairs)
        self.logger.info(f"probes à escuta em {self.bind_ip}:{self.port}")

    def stop(self):
        """Pára a receção e fecha o socket (idempotente). Espera no máximo PROBE_RECV_TIMEOUT."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        if self._sock is not None:
            self._sock.close()

    # --- Vizinhos ---

    def _on_neighbor_up(self, neighbor_id):
        with self.state.lock:
            neighbor = self.state.neighbors.get(neighbor_id)
            if neighbor is not None and neighbor.status == "up":  # pode já ter caído entretanto
                self.state.link_metrics[neighbor_id] = LinkMetrics()

    def _on_neighbor_down(self, neighbor_id, reason):
        with self.state.lock:
            # Se já há uma ligação nova a este vizinho, as métricas são dela
            neighbor = self.state.neighbors.get(neighbor_id)
            if neighbor is None or neighbor.status != "up":
                self.state.link_metrics.pop(neighbor_id, None)

    # --- Tarefas periódicas ---

    def send_probes(self):
        if self._stop.is_set():
            return
        for neighbor in self.state.up_neighbors():
            seq = self._next_seq
            self._next_seq += 1
            ts = time.monotonic()
            with self.state.lock:
                metrics = self.state.link_metrics.get(neighbor.id)
                if metrics is None:
                    continue
                # Registar antes de enviar: a resposta não pode chegar antes de o probe estar registado
                metrics.on_probe_sent(seq, ts)
            self._send(control.make_msg(control.PROBE, self.state.node_id, seq=seq, ts=ts),
                       (neighbor.ip, self.port))

    def send_pairs(self):
        """Envia a cada vizinho dois PROBE_PAIR seguidos, para ele estimar a capacidade do link."""
        if self._stop.is_set():
            return
        first, second = self._pair_datagrams(self._next_pair_id)
        self._next_pair_id += 1
        for neighbor in self.state.up_neighbors():
            # Já codificados: qualquer trabalho entre os dois envios seria medido como intervalo
            self._send_bytes(first, (neighbor.ip, self.port))
            self._send_bytes(second, (neighbor.ip, self.port))

    def _pair_datagrams(self, pair_id):
        """Os dois datagramas do par, cada um com PAIR_SIZE bytes."""
        datagrams = []
        for idx in (0, 1):
            msg = control.make_msg(control.PROBE_PAIR, self.state.node_id, pair_id=pair_id, idx=idx, pad="")
            msg["pad"] = PAD_CHAR * max(0, config.PAIR_SIZE - len(control.encode_udp(msg)))
            datagrams.append(control.encode_udp(msg))
        return datagrams

    def check_dead(self):
        if self._stop.is_set():
            return
        now = time.monotonic()
        dead = []
        with self.state.lock:
            for neighbor in self.state.up_neighbors():
                metrics = self.state.link_metrics.get(neighbor.id)
                if metrics is not None and metrics.silent_for(now) > config.DEAD_TIMEOUT:
                    dead.append(neighbor)
        # Fora do lock: fechar a ligação chama os ganchos on_neighbor_down
        for neighbor in dead:
            self.neighbors.close_neighbor(neighbor, DEAD_REASON)

    # --- Receção ---

    def _recv_loop(self):
        while not self._stop.is_set():
            try:
                data, addr = self._sock.recvfrom(config.PROBE_MAX_SIZE)
            except socket.timeout:
                continue
            except OSError as e:
                if not self._stop.is_set():
                    self.logger.warning(f"erro a receber probes: {e}")
                continue
            now = time.monotonic()  # antes de esperar pelo lock, para não inflacionar o RTT
            if self._stop.is_set():
                return  # parado durante a espera: um nó parado já não responde
            msg = _parse_probe(data)
            if msg is None:
                self.logger.debug(f"datagrama de probe inválido de {addr[0]}")
            elif msg["type"] == control.PROBE:
                # Responde a qualquer nó, mesmo que não seja vizinho (medição de candidatos na reparação).
                # Responde para o endereço de origem: quem mede pode estar numa porta qualquer
                self._send(control.make_msg(control.PROBE_REPLY, self.state.node_id,
                                            seq=msg["seq"], ts=msg.get("ts")), addr)
            elif msg["type"] == control.PROBE_REPLY:
                self._on_reply(msg, now)
            elif msg["type"] == control.PROBE_PAIR:
                self._on_pair(msg, now)

    def _on_reply(self, msg, now):
        # Identificar pelo src, nunca pelo IP de origem: no CORE um nó tem várias interfaces,
        # e a resposta pode sair por um IP diferente daquele para onde se enviou o probe
        with self.state.lock:
            metrics = self.state.link_metrics.get(msg["src"])
            if metrics is not None:
                metrics.on_reply(msg["seq"], now)

    def _on_pair(self, msg, now):
        with self.state.lock:
            metrics = self.state.link_metrics.get(msg["src"])
            if metrics is not None:
                metrics.on_pair_packet(msg["pair_id"], msg["idx"], now)

    def _send(self, msg, addr):
        self._send_bytes(control.encode_udp(msg), addr)

    def _send_bytes(self, data, addr):
        try:
            self._sock.sendto(data, addr)
        except OSError as e:
            # Ex.: rede inalcançável com um link em baixo: o probe conta como perdido
            self.logger.debug(f"probe para {addr[0]} não enviado: {e}")
