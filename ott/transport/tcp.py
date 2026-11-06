"""Transporte de controlo sobre TCP: prefixo de tamanho (ott.protocol.control), fila de envio, thread escritora e thread leitora.

Regra de concorrência: depois do HELLO, só a thread escritora do canal escreve no socket;
as outras threads usam send(), que apenas coloca a mensagem na fila.
"""

import queue
import socket
import threading
import time

from ott import config
from ott.protocol import control
from ott.transport.base import ControlChannel, ControlListener, ControlTransport, TransportError

# Colocado na fila de envio para acordar a thread escritora e fazê-la terminar
_STOP = object()


class TcpChannel(ControlChannel):
    def __init__(self, neighbor_id, peer_ip, sock, on_message, on_closed):
        super().__init__(neighbor_id, peer_ip, on_message, on_closed)
        self._sock = sock
        self._send_queue = queue.Queue(maxsize=config.SEND_QUEUE_MAX)
        self._closed = False
        self._closed_lock = threading.Lock()
        self._writer = None

    def start(self):
        try:
            self._sock.settimeout(None)
        except OSError:
            pass  # fechado entretanto: as threads vão terminar sozinhas
        self._writer = threading.Thread(target=self._writer_loop,
                                        name=f"writer-{self.neighbor_id}", daemon=True)
        self._writer.start()
        threading.Thread(target=self._reader_loop,
                         name=f"reader-{self.neighbor_id}", daemon=True).start()

    def respond(self, msg):
        try:
            control.send_msg(self._sock, msg)
        except (OSError, control.ProtocolError) as e:
            raise TransportError(str(e)) from e

    def send(self, msg):
        try:
            self._send_queue.put_nowait(msg)
            return True
        except queue.Full:
            return False

    def flush(self, timeout):
        if self.closed:
            return False
        done = threading.Event()
        if not self.send(done):
            return False
        return done.wait(timeout)

    def close(self):
        if self._mark_closed():
            self._release()

    @property
    def closed(self):
        with self._closed_lock:
            return self._closed

    def pending(self):
        return self._send_queue.qsize()

    def _mark_closed(self):
        """Devolve True só à primeira chamada: é o que garante um único on_closed."""
        with self._closed_lock:
            if self._closed:
                return False
            self._closed = True
            return True

    def _release(self):
        try:
            self._send_queue.put_nowait(_STOP)
        except queue.Full:
            pass  # a escritora não está bloqueada no get() e vê _closed na próxima mensagem
        try:
            # shutdown acorda uma thread leitora bloqueada em recv(); close() sozinho pode não acordar
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def _fail(self, reason):
        # Um erro causado pelo nosso próprio close() não é uma falha do vizinho
        if self._mark_closed():
            self._release()
            self._on_closed(self.neighbor_id, reason)

    def _writer_loop(self):
        while True:
            msg = self._send_queue.get()
            if msg is _STOP or self.closed:
                return
            if isinstance(msg, threading.Event):
                # Marcador de flush(): tudo o que estava antes na fila já foi escrito no socket
                msg.set()
                continue
            try:
                control.send_msg(self._sock, msg)
            except (OSError, control.ProtocolError):
                self._fail("erro de escrita")
                return

    def _reader_loop(self):
        while True:
            try:
                msg = control.recv_msg(self._sock)
                if msg is None:
                    self._fail("ligação fechada")
                    return
                control.validate(msg)
            except control.ProtocolError as e:
                self._fail(f"mensagem inválida: {e}")
                return
            except OSError as e:
                self._fail(f"erro de leitura: {e}")
                return
            if self.closed:
                return
            self._on_message(self.neighbor_id, msg)


class TcpListener(ControlListener):
    def __init__(self, bind_ip, port, on_channel, on_message, on_closed, logger):
        self._bind_ip = bind_ip
        self._port = port
        self._on_channel = on_channel
        self._on_message = on_message
        self._on_closed = on_closed
        self._logger = logger
        self._sock = None
        self._closing = False

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self._bind_ip, self._port))
        self._sock.listen()
        threading.Thread(target=self._accept_loop, name="control-accept", daemon=True).start()
        self._logger.info(f"controlo à escuta em {self._bind_ip}:{self._port}")

    def close(self):
        self._closing = True
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def _accept_loop(self):
        while True:
            try:
                conn, addr = self._sock.accept()
            except OSError as e:
                if self._closing:
                    return
                # Erro temporário (ex.: sem descritores livres): desistir deixava o nó sem aceitar vizinhos
                self._logger.warning(f"erro em accept() no controlo: {e}")
                time.sleep(config.ACCEPT_RETRY_DELAY)
                continue
            threading.Thread(target=self._read_hello, args=(conn, addr),
                             name=f"hello-{addr[0]}", daemon=True).start()

    def _read_hello(self, conn, addr):
        # O timeout mantém-se até start(), por isso também limita o envio da resposta ao HELLO
        conn.settimeout(config.HELLO_TIMEOUT)
        try:
            msg = control.recv_msg(conn)
            if msg is None:
                conn.close()
                return
            control.validate(msg)
        except (OSError, control.ProtocolError) as e:
            self._logger.warning(f"ligação de {addr[0]} sem HELLO válido: {e}")
            conn.close()
            return
        if msg["type"] != control.HELLO:
            self._logger.warning(f"esperava HELLO de {addr[0]}, recebi {msg['type']}")
            conn.close()
            return
        channel = TcpChannel(msg["src"], addr[0], conn, self._on_message, self._on_closed)
        self._on_channel(channel, msg)


class TcpTransport(ControlTransport):
    def __init__(self, on_message, on_closed, logger, bind_ip=config.ANY_IP):
        super().__init__(on_message, on_closed)
        self._logger = logger
        self._bind_ip = bind_ip

    def listen(self, port, on_channel):
        return TcpListener(self._bind_ip, port, on_channel, self._on_message, self._on_closed, self._logger)

    def connect(self, ip, port, hello_msg, timeout):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # Sair pelo IP --bind: o vizinho vê-nos com o IP certo (essencial nos testes em 127.0.0.x)
            if self._bind_ip != config.ANY_IP:
                sock.bind((self._bind_ip, 0))
            sock.settimeout(timeout)
            sock.connect((ip, port))
            control.send_msg(sock, hello_msg)
            reply = control.recv_msg(sock)
            if reply is None:
                raise OSError("ligação fechada antes da resposta")
            control.validate(reply)
        except (OSError, control.ProtocolError) as e:
            sock.close()
            raise TransportError(str(e)) from e
        return TcpChannel(reply["src"], ip, sock, self._on_message, self._on_closed), reply
