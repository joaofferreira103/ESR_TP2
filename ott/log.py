"""Logging do nó: escreve no terminal e em logs/<node_id>.log, com o formato "HH:MM:SS.mmm [O5] NIVEL mensagem"."""

import logging
import os

LOG_DIR = "logs"


def get_logger(node_id):
    """Devolve o logger do nó, criando-o na primeira chamada."""
    logger = logging.getLogger(f"ott.{node_id}")
    # Chamadas repetidas devolvem o mesmo logger sem duplicar os handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt=f"%(asctime)s.%(msecs)03d [{node_id}] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    os.makedirs(LOG_DIR, exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"{node_id}.log"), encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
