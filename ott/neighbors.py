"""Gestão de vizinhos: HELLO/HELLO_ACK/HELLO_REJECT/LEAVE e reconexão, sobre a camada de transporte (ott.transport.base).

Ciclo de vida de uma ligação:
- Saída (connect_to): o vizinho é reservado no estado como "connecting", o transporte envia o
  HELLO e devolve o canal e a resposta. Com HELLO_ACK passa a "up".
- Entrada (_handle_incoming): o listener entrega o canal com o HELLO já lido; decide-se (aceitar
  ou rejeitar) com o lock e responde-se sem o lock. Ao aceitar, o vizinho entra logo como "up".
- Depois do HELLO, as mensagens recebidas chegam a _on_channel_message, que as despacha para os
  handlers registados.
- Remoção (_drop): acontece uma única vez por vizinho, mesmo que várias falhas cheguem ao mesmo
  tempo, porque só remove se o objeto no estado ainda for o mesmo.
"""

import threading
import time

from ott import config
from ott.protocol import control
from ott.state import Neighbor
from ott.transport.base import TransportError


class NeighborManager:
    def __init__(self, state, logger, transport_factory, port=config.CONTROL_PORT):
        """transport_factory(on_message, on_closed) devolve o ControlTransport a usar."""
        self.state = state
        self.logger = logger
        self.transport = transport_factory(self._on_channel_message, self._on_channel_closed)
        self.port = port            # porta de controlo (a mesma em todos os nós)
        self._handlers = {}         # tipo de mensagem -> função(neighbor_id, msg)
        self._up_listeners = []     # funções(neighbor_id)
        self._down_listeners = []   # funções(neighbor_id, motivo)
        self._listener = None
        self._stopping = False

    # --- Ganchos para as fases seguintes ---

    def register_handler(self, msg_type, func):
        """Regista func(neighbor_id, msg) para as mensagens do tipo msg_type (ANNOUNCE, ACTIVATE, ...)."""
        self._handlers[msg_type] = func

    def add_up_listener(self, func):
        """func(neighbor_id) é chamada quando um vizinho fica "up" (Fase 3: enviar os últimos ANNOUNCE)."""
        self._up_listeners.append(func)

    def add_down_listener(self, func):
        """func(neighbor_id, motivo) é chamada quando um vizinho "up" é removido
        (Fases 2, 3, 4 e 6: limpar candidatos, trocar de upstream, autorreparação)."""
        self._down_listeners.append(func)

    def on_neighbor_up(self, neighbor_id):
        self.logger.debug(f"evento: vizinho {neighbor_id} up")
        for func in self._up_listeners:
            self._call_listener(func, neighbor_id)

    def on_neighbor_down(self, neighbor_id, reason):
        self.logger.debug(f"evento: vizinho {neighbor_id} down ({reason})")
        for func in self._down_listeners:
            self._call_listener(func, neighbor_id, reason)

    def _call_listener(self, func, *args):
        try:
            func(*args)
        except Exception:
            self.logger.exception(f"erro num gancho de vizinhos ({func.__name__})")

    # --- Arranque e paragem ---

    def start(self):
        """Abre o listener de controlo, que passa a entregar as ligações de entrada."""
        self._listener = self.transport.listen(self.port, self._handle_incoming)
        self._listener.start()

    def stop(self):
        """Terminação limpa: LEAVE a todos os vizinhos "up", espera que saiam e fecha tudo."""
        with self.state.lock:
            self._stopping = True
            # Retirar todos do estado primeiro: os erros das threads de leitura que se seguem
            # ao fecho já não encontram o vizinho e não são tratados como falhas
            neighbors = list(self.state.neighbors.values())
            self.state.neighbors.clear()
        if self._listener is not None:
            self._listener.close()

        up = [n for n in neighbors if n.status == "up"]
        for n in up:
            n.channel.send(control.make_msg(control.LEAVE, self.state.node_id))
        deadline = time.monotonic() + config.LEAVE_FLUSH_TIMEOUT
        for n in up:
            n.channel.flush(max(0.0, deadline - time.monotonic()))
        for n in neighbors:
            n.close()
        self.logger.info(f"LEAVE enviado a {len(up)} vizinhos; ligações fechadas")

    # --- Envio ---

    def send_to(self, neighbor_id, msg):
        """Coloca msg na fila do vizinho. Fila cheia = vizinho lento: a ligação é fechada."""
        with self.state.lock:
            neighbor = self.state.neighbors.get(neighbor_id)
            if neighbor is None or neighbor.status != "up":
                return False
        if not neighbor.channel.send(msg):
            self.logger.warning(f"vizinho {neighbor_id} lento (fila de envio cheia)")
            self._drop(neighbor, "fila de envio cheia")
            return False
        return True

    def close_neighbor(self, neighbor, reason):
        """Fecha a ligação a um vizinho falhado (chama on_neighbor_down). Recebe o objeto Neighbor,
        e não o id, para nunca fechar uma ligação nova ao mesmo vizinho."""
        self._drop(neighbor, reason)

    # --- Ligações de saída ---

    def connect_to(self, neighbor_id, ip):
        """Liga-se a um vizinho e faz o HELLO (bloqueia até HELLO_TIMEOUT). Devolve True se ficou "up"."""
        neighbor = self._reserve(neighbor_id, ip)
        if neighbor is None:
            return False
        return self._handshake(neighbor)

    def connect_async(self, neighbor_id, ip):
        """Como connect_to, mas o HELLO corre numa thread própria (para não bloquear o scheduler)."""
        neighbor = self._reserve(neighbor_id, ip)
        if neighbor is not None:
            threading.Thread(target=self._handshake, args=(neighbor,),
                             name=f"connect-{neighbor_id}", daemon=True).start()

    def reconnect_static(self):
        """Tarefa periódica: nova tentativa de ligação aos vizinhos estáticos sem ligação."""
        with self.state.lock:
            if self.state.count_up() >= config.X_MAX:
                return
            missing = {nid: ip for nid, ip in self.state.static_neighbors.items()
                       if nid not in self.state.neighbors}
        for neighbor_id, ip in missing.items():
            self.logger.info(f"nova tentativa de ligação a {neighbor_id} ({ip})")
            self.connect_async(neighbor_id, ip)

    def _reserve(self, neighbor_id, ip):
        """Regista o vizinho como "connecting" (evita duas tentativas em paralelo para o mesmo id)."""
        with self.state.lock:
            if self._stopping or neighbor_id == self.state.node_id or neighbor_id in self.state.neighbors:
                return None
            if self.state.count_up() >= config.X_MAX:
                return None  # X_MAX também limita as ligações de saída, não só os HELLO recebidos
            neighbor = Neighbor(neighbor_id, ip, initiated_by_me=True,
                                static=neighbor_id in self.state.static_neighbors)
            self.state.neighbors[neighbor_id] = neighbor
            return neighbor

    def _handshake(self, neighbor):
        hello = control.make_msg(control.HELLO, self.state.node_id)
        try:
            channel, reply = self.transport.connect(neighbor.ip, self.port, hello, config.HELLO_TIMEOUT)
        except TransportError as e:
            self._abort(neighbor, f"falhou: {e}")
            return False
        neighbor.channel = channel

        if reply["type"] == control.HELLO_REJECT:
            self._abort(neighbor, f"rejeitada: {reply.get('reason')}")
            return False
        if reply["type"] != control.HELLO_ACK or reply["src"] != neighbor.id:
            self._abort(neighbor, f"resposta inesperada: {reply['type']} de {reply['src']}")
            return False

        with self.state.lock:
            still_mine = self.state.neighbors.get(neighbor.id) is neighbor
            if still_mine:
                neighbor.status = "up"
                neighbor.connected_since = time.time()
        if not still_mine:
            # Entretanto ficou outra ligação (simultânea) ou o nó está a terminar
            neighbor.close()
            return False
        self._activate(neighbor)
        return True

    def _abort(self, neighbor, reason):
        """Desiste de uma ligação de saída que não chegou a "up"."""
        with self.state.lock:
            removed = self.state.neighbors.get(neighbor.id) is neighbor
            if removed:
                del self.state.neighbors[neighbor.id]
        neighbor.close()
        if removed:
            self.logger.warning(f"ligação a {neighbor.id} ({neighbor.ip}) {reason}")
        elif not self._stopping:
            self.logger.info(f"ligação de saída a {neighbor.id} abandonada "
                             f"(ficou a ligação iniciada por {neighbor.id}, ID menor)")

    # --- Ligações de entrada ---

    def _handle_incoming(self, channel, hello):
        peer_id = hello["src"]
        neighbor, reject_reason = self._decide(peer_id, channel)
        if neighbor is None:
            if reject_reason is not None:
                self._reply_and_close(channel, peer_id, reject_reason)
            else:
                channel.close()
            return

        try:
            channel.respond(control.make_msg(control.HELLO_ACK, self.state.node_id))
        except TransportError as e:
            self._drop(neighbor, f"erro ao enviar HELLO_ACK: {e}")
            return
        self._activate(neighbor)

    def _decide(self, peer_id, channel):
        """Decide (com o lock) se aceita o HELLO. Devolve (Neighbor aceite, None) ou (None, motivo)."""
        with self.state.lock:
            if self._stopping:
                return None, None
            existing = self.state.neighbors.get(peer_id)
            if existing is not None and existing.status == "up":
                return None, "duplicate"
            if existing is not None:
                # Os dois ligaram-se ao mesmo tempo: fica a ligação iniciada pelo ID menor
                if peer_id < self.state.node_id:
                    self.logger.info(f"ligação simultânea com {peer_id}: fica a dele (ID menor)")
                    # A minha tentativa é abandonada: a thread de saída vê que deixou de estar no estado
                else:
                    self.logger.info(f"ligação simultânea com {peer_id}: fica a minha (ID menor)")
                    return None, "duplicate"
            elif self.state.count_up() >= config.X_MAX:
                return None, "full"

            # IP: o da lista estática se existir; senão, o IP de onde veio a ligação
            ip = self.state.static_neighbors.get(peer_id, channel.peer_ip)
            neighbor = Neighbor(peer_id, ip, channel=channel, initiated_by_me=False,
                                static=peer_id in self.state.static_neighbors)
            neighbor.status = "up"
            neighbor.connected_since = time.time()
            self.state.neighbors[peer_id] = neighbor
            return neighbor, None

    def _reply_and_close(self, channel, peer_id, reason):
        try:
            channel.respond(control.make_msg(control.HELLO_REJECT, self.state.node_id, reason=reason))
        except TransportError as e:
            self.logger.debug(f"HELLO_REJECT para {peer_id} não enviado: {e}")
        channel.close()
        self.logger.info(f"HELLO de {peer_id} rejeitado ({reason})")

    # --- Vizinho "up": eventos do canal ---

    def _activate(self, neighbor):
        """Arranca o canal do vizinho, já "up"."""
        if neighbor.channel.closed:
            return  # removido entretanto (o nó está a terminar)
        neighbor.channel.start()
        who = "por mim" if neighbor.initiated_by_me else f"por {neighbor.id}"
        self.logger.info(f"ligação a {neighbor.id} ({neighbor.ip}) estabelecida, iniciada {who}")
        self.on_neighbor_up(neighbor.id)

    def _on_channel_message(self, neighbor_id, msg):
        if msg["type"] == control.LEAVE:
            neighbor = self.state.get_neighbor(neighbor_id)
            if neighbor is not None:
                self._drop(neighbor, "saiu", left=True)
            return
        handler = self._handlers.get(msg["type"])
        if handler is None:
            self.logger.warning(f"mensagem {msg['type']} de {neighbor_id} sem handler")
            return
        try:
            handler(neighbor_id, msg)
        except Exception:
            self.logger.exception(f"erro a tratar {msg['type']} de {neighbor_id}")

    def _on_channel_closed(self, neighbor_id, reason):
        """O canal que falhou já está fechado. Se o canal atual deste vizinho estiver aberto, o aviso
        é de uma ligação antiga, já substituída por outra com o mesmo id, e não a pode derrubar."""
        neighbor = self.state.get_neighbor(neighbor_id)
        if neighbor is None or neighbor.channel is None or not neighbor.channel.closed:
            return
        self._drop(neighbor, reason)

    def _drop(self, neighbor, reason, left=False):
        """Remove um vizinho e fecha a ligação. Chamado pelos eventos do canal ou por send_to;
        só a primeira chamada tem efeito (as seguintes já não encontram o objeto no estado)."""
        with self.state.lock:
            if self.state.neighbors.get(neighbor.id) is not neighbor:
                return
            del self.state.neighbors[neighbor.id]
            was_up = neighbor.status == "up"
        neighbor.close()
        if left:
            self.logger.info(f"{neighbor.id} saiu")
        else:
            self.logger.warning(f"ligação a {neighbor.id} perdida ({reason})")
        if was_up:
            self.on_neighbor_down(neighbor.id, reason)
