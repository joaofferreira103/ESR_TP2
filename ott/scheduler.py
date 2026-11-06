"""Thread de temporizadores: executa tarefas periódicas (novas ligações, dump do estado e, nas fases
seguintes, anúncios, probes, renovações e expirações).

Todas as tarefas correm na mesma thread, uma de cada vez: têm de ser rápidas e não podem bloquear
(enviar por TCP através das filas dos vizinhos, nunca diretamente no socket).
"""

import threading
import time

IDLE_WAIT = 1.0     # s: espera máxima sem tarefas registadas (add_periodic acorda a thread)


class _Task:
    def __init__(self, name, interval, func, next_run):
        self.name = name
        self.interval = interval
        self.func = func
        self.next_run = next_run


class Scheduler:
    def __init__(self, logger):
        self.logger = logger
        self._tasks = []
        # Lock próprio (não o global): protege só a lista de tarefas e serve para acordar a thread
        self._cond = threading.Condition()
        self._running = False
        self._thread = None

    def add_periodic(self, name, interval, func):
        """Regista func() para correr a cada interval segundos. A primeira execução é ao fim de um intervalo."""
        with self._cond:
            self._tasks.append(_Task(name, interval, func, time.monotonic() + interval))
            self._cond.notify()  # a nova tarefa pode ser a próxima a correr

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)
        self._thread.start()

    def stop(self):
        with self._cond:
            self._running = False
            self._cond.notify()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join()

    def _loop(self):
        while True:
            with self._cond:
                if not self._running:
                    return
                now = time.monotonic()
                due = [t for t in self._tasks if t.next_run <= now]
                if not due:
                    # Dorme até à próxima tarefa (ou até ser acordado por add_periodic/stop)
                    timeout = min((t.next_run for t in self._tasks), default=now + IDLE_WAIT) - now
                    self._cond.wait(timeout)
                    continue
            # As tarefas correm fora do lock do scheduler
            for task in due:
                self._run(task)

    def _run(self, task):
        try:
            task.func()
        except Exception:
            self.logger.exception(f"erro na tarefa periódica '{task.name}'")
        # Ritmo fixo (next_run += interval) para não acumular desvio; se a tarefa se atrasou
        # mais do que um intervalo, salta as execuções perdidas em vez de as fazer todas seguidas
        with self._cond:
            task.next_run += task.interval
            now = time.monotonic()
            if task.next_run < now:
                task.next_run = now + task.interval
