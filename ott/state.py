"""Estado partilhado do nó (vizinhos, candidatos, fluxos), protegido por um único lock global (RLock).

Regras de concorrência:
- Todo o acesso ao estado partilhado é feito com NodeState.lock adquirido.
- Nunca fazer I/O de rede com o lock global adquirido: copiar o que é preciso,
  libertar o lock e só depois enviar. Um envio lento não pode bloquear o nó inteiro.
- Os métodos auxiliares adquirem o lock internamente e devolvem cópias, para
  quem chama poder iterar sem o lock.
- Os envios para um vizinho passam sempre pelo seu ControlChannel (ott.transport.base).
"""

import threading
import time


class Neighbor:
    """Um vizinho overlay e o seu canal de controlo (None enquanto a ligação de saída não está feita)."""

    def __init__(self, neighbor_id, ip, channel=None, initiated_by_me=False, static=False):
        self.id = neighbor_id
        self.ip = ip
        self.channel = channel
        self.status = "connecting"          # "connecting" | "up"
        self.initiated_by_me = initiated_by_me
        self.static = static                # True se veio da lista do bootstrapper
        self.connected_since = None         # instante (time.time()) em que ficou "up"

    def pending(self):
        """Mensagens à espera de envio."""
        return self.channel.pending() if self.channel is not None else 0

    def close(self):
        """Fecha o canal, se existir. Não adquire o lock global."""
        if self.channel is not None:
            self.channel.close()

    def __repr__(self):
        return f"Neighbor({self.id}, {self.ip}, {self.status})"


class NodeState:
    """Estado partilhado de um nó overlay."""

    def __init__(self, node_id):
        self.node_id = node_id
        self.lock = threading.RLock()
        self.neighbors = {}           # id -> Neighbor
        self.static_neighbors = {}    # id -> ip, lista recebida do bootstrapper
        self.link_metrics = {}        # id -> LinkMetrics, um por vizinho "up" (ott.metrics)
        # Vizinhos dos vizinhos, para a reparação (ott.repair): id -> {"ip", "via", "last_seen"}
        self.overlay_candidates = {}

        # Gancho para a Fase 3 (encaminhamento):
        # candidates[servidor][vizinho] = {"epoch", "seq", "last_seen",
        #     "streams": {stream: {"cost", "hops", "active", "via"}}}
        # o que cada vizinho anunciou, com o custo do link já somado.
        self.candidates = {}

        # Gancho para a Fase 4 (ativação e plano de dados):
        # flows[fluxo] = {"upstream", "new_upstream" (só durante make-before-break),
        #     "destinations": {vizinho ou "local": instante_expiração},
        #     "state": "inactive" | "active"}
        self.flows = {}

    def add_neighbor(self, neighbor):
        """Adiciona um vizinho. Devolve False se já existir um com o mesmo id (quem chama decide)."""
        with self.lock:
            if neighbor.id in self.neighbors:
                return False
            self.neighbors[neighbor.id] = neighbor
            return True

    def remove_neighbor(self, neighbor_id):
        """Remove e devolve o vizinho (ou None). Quem chama fecha-o DEPOIS, sem o lock."""
        with self.lock:
            return self.neighbors.pop(neighbor_id, None)

    def get_neighbor(self, neighbor_id):
        with self.lock:
            return self.neighbors.get(neighbor_id)

    def up_neighbors(self):
        """Devolve uma cópia da lista de vizinhos "up" (os objetos Neighbor são os mesmos)."""
        with self.lock:
            return [n for n in self.neighbors.values() if n.status == "up"]

    def count_up(self):
        with self.lock:
            return sum(1 for n in self.neighbors.values() if n.status == "up")

    def snapshot(self):
        """Texto legível do estado, para os logs."""
        with self.lock:
            now = time.time()
            lines = [f"Estado de {self.node_id}: {len(self.neighbors)} vizinhos "
                     f"({self.count_up()} up), {len(self.static_neighbors)} estáticos"]
            for n in sorted(self.neighbors.values(), key=lambda n: n.id):
                age = f"{now - n.connected_since:.0f}s" if n.connected_since else "-"
                lines.append(
                    f"  {n.id} {n.ip} {n.status} "
                    f"{'estático' if n.static else 'dinâmico'} "
                    f"{'iniciada por mim' if n.initiated_by_me else 'iniciada pelo vizinho'} "
                    f"há {age} fila={n.pending()}"
                )
            missing = sorted(set(self.static_neighbors) - set(self.neighbors))
            if missing:
                lines.append(f"  estáticos sem ligação: {', '.join(missing)}")
            return "\n".join(lines)
