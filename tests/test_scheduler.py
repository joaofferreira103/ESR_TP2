"""Testes da thread de temporizadores."""

import logging
import threading
import time
import unittest

from ott.scheduler import Scheduler


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("ott.test.scheduler")
        self.scheduler = Scheduler(self.logger)

    def tearDown(self):
        self.scheduler.stop()

    def test_periodic_count(self):
        calls = []
        self.scheduler.add_periodic("contar", 0.05, lambda: calls.append(time.monotonic()))
        self.scheduler.start()
        time.sleep(0.52)
        self.scheduler.stop()
        # 0,52 s / 0,05 s ≈ 10 execuções; margem para a imprecisão do sleep
        self.assertGreaterEqual(len(calls), 8)
        self.assertLessEqual(len(calls), 11)

    def test_exception_does_not_stop_others(self):
        good_calls = []
        failed = threading.Event()

        def bad():
            failed.set()
            raise RuntimeError("falha de teste")

        self.scheduler.add_periodic("má", 0.05, bad)
        self.scheduler.add_periodic("boa", 0.05, lambda: good_calls.append(1))
        with self.assertLogs(self.logger, level="ERROR") as logs:
            self.scheduler.start()
            time.sleep(0.32)
            self.scheduler.stop()
        self.assertTrue(failed.is_set())
        self.assertGreaterEqual(len(good_calls), 4)
        self.assertIn("má", logs.output[0])

    def test_add_while_running(self):
        calls = []
        self.scheduler.start()
        self.scheduler.add_periodic("tardia", 0.05, lambda: calls.append(1))
        time.sleep(0.27)
        self.scheduler.stop()
        self.assertGreaterEqual(len(calls), 3)

    def test_stop_without_tasks(self):
        self.scheduler.start()
        start = time.monotonic()
        self.scheduler.stop()
        self.assertLess(time.monotonic() - start, 0.5)


if __name__ == "__main__":
    unittest.main()
