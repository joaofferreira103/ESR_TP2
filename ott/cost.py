"""Função de custo de um link, em milissegundos. Vive num módulo próprio para se poderem comparar modos nos testes."""

from ott import config

MS_PER_SECOND = 1000
BITS_PER_MBIT = 1_000_000


def link_cost(metrics, mode=config.COST_MODE, now=None):
    """Custo do link descrito por metrics (LinkMetrics), em ms. Chamar com o lock global adquirido.

    Modo "full":
        SRTT/2 + K_JITTER × RTTVAR + K_LOSS_MS × perda + W_BW_MS × BW_REQUIRED / capacidade + HOP_COST

    Cada métrica é convertida numa penalização em ms ("atraso equivalente") e soma-se, para a perda e a
    falta de capacidade contarem mesmo quando o atraso é quase nulo (caso do CORE sem atraso configurado).

    - SRTT/2: atraso estimado num só sentido, assumindo um link simétrico.
    - K_JITTER × RTTVAR: o atraso extra que o cliente tem de absorver no buffer de reprodução.
    - K_LOSS_MS × perda: 10% de perda equivale a +10 ms.
    - W_BW_MS × BW_REQUIRED / capacidade: uma capacidade igual ao débito necessário equivale a +10 ms,
      e o termo cresce quando a capacidade é menor. Com a capacidade ainda desconhecida vale 0.
    - HOP_COST: custo fixo por salto: desempata pelo número de saltos e mantém o custo positivo
      (necessário para não haver ciclos no flooding).

    Alternativa rejeitada: a fórmula multiplicativa (SRTT/2 + K_JITTER × RTTVAR) × (1 + K_LOSS × perda)
    × (1 + W_BW × BW_REQUIRED / capacidade) + HOP_COST. Como os fatores multiplicam só o atraso e o
    jitter, quase não pesam quando estes são quase nulos: 18% de perda custava só 1,84 ms, e a rota
    preferia esse link a um link limpo com 5 ms de atraso.

    Modo "latency": SRTT/2 + HOP_COST. Modo "hops": HOP_COST, equivale a contar saltos.
    Os dois servem para medir nos testes o ganho de usar as métricas completas.

    Um link que ainda não está pronto (is_ready falso) custa sempre DEFAULT_LINK_COST, em qualquer
    modo, para não se preferirem links desconhecidos.

    Unidades: custo em ms, capacidade e BW_REQUIRED em bits/s.
    """
    if mode not in ("full", "latency", "hops"):
        raise ValueError(f"modo de custo desconhecido: {mode}")
    if not metrics.is_ready():
        return config.DEFAULT_LINK_COST
    if mode == "hops":
        return config.HOP_COST

    delay = metrics.srtt / 2 * MS_PER_SECOND
    if mode == "latency":
        return delay + config.HOP_COST

    jitter = config.K_JITTER * metrics.rttvar * MS_PER_SECOND
    loss = config.K_LOSS_MS * metrics.loss(now)
    capacity = metrics.capacity()
    bw_required = config.BW_REQUIRED_MBPS * BITS_PER_MBIT
    bandwidth = 0 if capacity is None else config.W_BW_MS * bw_required / capacity
    return delay + jitter + loss + bandwidth + config.HOP_COST
