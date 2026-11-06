"""Interfaces da camada de transporte do controlo. O resto do nó só conhece estas classes.

Garantias que qualquer implementação tem de dar a quem a usa:
- Entrega fiável e por ordem, sem duplicados, das mensagens de controlo de cada canal.
- send() nunca bloqueia: uma ligação lenta não pode atrasar o resto do nó.
- on_closed é chamado no máximo uma vez por canal, só para falhas (nunca por um close() local),
  e o canal já está marcado como fechado (closed == True) quando é chamado.
- Os callbacks não adquirem locks do transporte, por isso podem chamar send() e close().

Ciclo de vida de um canal: criado por connect() ou entregue pelo listener, responde ao HELLO
(só no lado que o recebeu), start() arranca a entrega de mensagens, close() termina.
"""

from abc import ABC, abstractmethod


class TransportError(Exception):
    """Falha numa operação de transporte (ligar, enviar a resposta ao HELLO, ...)."""


class ControlChannel(ABC):
    """Ligação de controlo a UM vizinho.

    on_message(neighbor_id, msg): mensagem recebida (já validada).
    on_closed(neighbor_id, reason): a ligação falhou ou foi fechada pelo vizinho.
    """

    def __init__(self, neighbor_id, peer_ip, on_message, on_closed):
        self.neighbor_id = neighbor_id
        self.peer_ip = peer_ip          # IP de onde veio a ligação
        self._on_message = on_message
        self._on_closed = on_closed

    @abstractmethod
    def start(self):
        """Arranca o envio e a receção. Os callbacks só são chamados depois disto."""

    @abstractmethod
    def respond(self, msg):
        """Envia já a resposta ao HELLO, antes de start(). Lança TransportError se falhar."""

    @abstractmethod
    def send(self, msg):
        """Põe a mensagem na fila, sem bloquear. Devolve False se não foi possível."""

    @abstractmethod
    def flush(self, timeout):
        """Espera (no máximo timeout s) que o que está na fila seja enviado. Devolve True se conseguiu."""

    @abstractmethod
    def close(self):
        """Fecha o canal (idempotente), sem chamar on_closed."""

    @property
    @abstractmethod
    def closed(self):
        """True se o canal já foi fechado (localmente ou por falha)."""

    @abstractmethod
    def pending(self):
        """Número de mensagens à espera de envio (para os logs)."""


class ControlListener(ABC):
    """Aceita ligações de entrada e entrega on_channel(canal, hello), com o HELLO já lido e validado.

    on_channel pode bloquear (corre numa thread própria por ligação). Depois de decidir,
    tem de chamar channel.respond() e channel.start(), ou channel.close().
    """

    @abstractmethod
    def start(self):
        """Começa a aceitar ligações."""

    @abstractmethod
    def close(self):
        """Deixa de aceitar ligações (idempotente)."""


class ControlTransport(ABC):
    """Cria canais e listeners. Os callbacks dados aqui são entregues a cada canal na sua criação."""

    def __init__(self, on_message, on_closed):
        self._on_message = on_message
        self._on_closed = on_closed

    @abstractmethod
    def listen(self, port, on_channel):
        """Devolve um ControlListener (ainda por arrancar) nesta porta."""

    @abstractmethod
    def connect(self, ip, port, hello_msg, timeout):
        """Liga-se, envia hello_msg e espera pela resposta. Bloqueia no máximo timeout s.

        Devolve (canal ainda por arrancar, resposta validada). Lança TransportError se falhar.
        """
