# ESR TP2 — Serviço OTT

Rede overlay multicast na camada de aplicação para streaming de vídeo MJPEG sobre o emulador CORE.

## Ficheiro de topologia

O bootstrapper (`python3 -m ott.bootstrapper <ficheiro.json> [--bind <ip>]`) lê a topologia de um ficheiro JSON:

```json
{
  "nodes": {
    "O1": {"ip": "10.0.0.1", "neighbors": ["O2"]},
    "O2": {"ip": "10.0.1.2", "neighbors": ["O1", "O3"]},
    "O3": {"ip": "10.0.2.3", "neighbors": ["O2"]}
  }
}
```

- Cada chave de `nodes` é o ID do nó (o mesmo que se passa a `ott.node`).
- `ip`: o IP que os vizinhos usam para se ligarem a esse nó.
- `neighbors`: IDs dos vizinhos que o bootstrapper devolve a esse nó no REGISTER. Todos têm de existir no ficheiro; a relação deve ser simétrica (o bootstrapper avisa se não for).

Ficheiros em `configs/`: `local_*.json` para testes numa só máquina (IPs 127.0.0.11, 127.0.0.12, ...) e `core_*.json` para o CORE (substituir os `<IP_On>` pelos IPs reais; o bootstrapper recusa arrancar com IPs inválidos).

## Como correr localmente

Numa só máquina, cada nó usa o seu IP de loopback (127.0.0.11, 127.0.0.12, ...) e o bootstrapper 127.0.0.1:

```bash
scripts/run_local.sh configs/local_cenario2.json              # bootstrapper + todos os nós, em background
scripts/run_local.sh configs/local_cenario2.json --reverse --boot-last   # nós por ordem inversa, bootstrapper no fim

echo vizinhos > logs/O3.cmd     # o estado de O3 aparece em logs/O3.out
echo metricas > logs/O3.cmd     # SRTT, RTTVAR, perda, amostras, capacidade e custo por vizinho
echo candidatos > logs/O3.cmd   # candidatos de reparação (vizinhos dos vizinhos)
echo sair > logs/O3.cmd         # terminação limpa de O3 (envia LEAVE)
kill -9 $(cat logs/O3.pid)      # simular a falha (crash) de O3
scripts/start_node.sh configs/local_cenario2.json O3          # voltar a arrancar O3

scripts/stop_local.sh           # termina tudo (ou: scripts/stop_local.sh O3 O4)
```

Em `logs/`: `<ID>.log` é o log do nó; `<ID>.out` é a saída do processo (inclui a resposta a `vizinhos` e eventuais tracebacks); `<ID>.cmd` é um FIFO ligado ao stdin do nó.

## Como correr no CORE

1. Abrir no CORE a topologia do cenário, guardada em `topologias/` (ex.: `topologias/cenario2.imn`). A topologia tem de ter um **host dedicado ao bootstrapper**, afastado do overlay. Esse host não é um nó overlay: não entra em `configs/core_cenarioN.json` e não corre `ott.node`.
2. Confirmar que os IPs em `configs/core_cenarioN.json` correspondem aos dos hosts escolhidos para cada nó overlay (substituir os `<IP_On>`; usar o IP da interface pela qual os vizinhos chegam ao host).
3. Iniciar a emulação (botão verde "Start the session").
4. Abrir um terminal em cada host overlay (duplo clique no host) e, em todos, ir para a pasta do projeto: `cd <caminho>/ESR_TP2` (os hosts do CORE veem o sistema de ficheiros da máquina).
5. No host dedicado ao bootstrapper:
   ```bash
   python3 -m ott.bootstrapper configs/core_cenario2.json
   ```
6. Em cada host overlay, com o seu ID (a ordem não importa; um nó arrancado antes do bootstrapper tenta de novo a cada 2 s):
   ```bash
   python3 -m ott.node O1 <IP_do_bootstrapper> --server
   python3 -m ott.node O2 <IP_do_bootstrapper>
   python3 -m ott.node O5 <IP_do_bootstrapper> --client
   ```
   No CORE não é preciso `--bind` (cada host tem os seus IPs). No terminal do nó: `vizinhos` mostra o estado, `metricas` mostra SRTT, RTTVAR, perda, amostras, capacidade e custo por vizinho, `candidatos` mostra os candidatos de reparação, `sair` (ou Ctrl+C) termina.
7. Os logs ficam em `logs/<ID>.log` na pasta do projeto.

### Arranque automático no CORE (`scripts/core.sh`)

Com a sessão do CORE já arrancada (botão verde), num terminal **normal da VM** (não é preciso abrir o terminal de cada nó). Topologia: `topologias/topologia_ott.xml`; overlay: `configs/core_ott.json` (cada nó tem `core_node`, o nó do CORE onde corre, e opcionalmente `args`, ex.: `["--server"]`).

```bash
sudo scripts/core.sh start              # espera pelo OSPF, arranca o bootstrapper (n25) e todos os nós
sudo scripts/core.sh status
sudo scripts/core.sh cmd O3 vizinhos    # também: metricas, candidatos, sair
sudo scripts/core.sh freeze O6          # kill -STOP (falha silenciosa); unfreeze O6 para retomar
sudo scripts/core.sh crash O6           # kill -9; sudo scripts/core.sh start O6 para voltar a arrancar
sudo scripts/core.sh stop               # termina tudo (ou: stop O3 O4)
```

## Testes à mão no CORE

Configurar um link de cada vez (duplo clique no link) e ler o resultado com `metricas` no nó da ponta do link. Os valores "Medido" vêm de um *network namespace* com `tc` em `lo`, onde cada pacote passa pelo limite nos dois sentidos; ainda não foram confirmados no CORE. Contexto em `docs/fase2.md`.

| # | Configurar | Resultado esperado | Medido | Atenção |
|---|---|---|---|---|
| 1 | 20 ms de atraso num link | SRTT desse link sobe cerca de 40 ms em poucos segundos; o custo sobe cerca de 20 ms | SRTT 40,4 ms, custo 21,4 ms | Se o atraso só se aplicar a um sentido, o SRTT sobe só 20 ms |
| 2 | Jitter num link | RTTVAR sobe | Com 5 ms de jitter: RTTVAR de 3 a 4 ms | A capacidade estimada pode descer para valores falsos (1,3 a 1,9 Mbit/s) ou ficar sem estimativa. Com 20 ms de atraso e 5 ms de jitter, o custo foi de 37 ms, dos quais cerca de 10 ms vêm dessa capacidade falsa |
| 3 | 10% de perda | Perda de cerca de 0,1 se a perda só se aplicar a um sentido, ou de cerca de 0,19 se se aplicar aos dois (1 − 0,9²), ao fim de cerca de 13 s (10 s de janela + 3 s até um probe contar como perdido) | 12% a 19%, cerca de 19% acumulado; custo de 20,5 ms com 19% de perda | A janela de 50 probes oscila cerca de 5 pontos. O custo sobe 1 ms por cada 1% de perda, mesmo com atraso quase nulo |
| 4 | Limitar um link a 1 Mbps | Capacidade cerca de 1 Mbps (esperar até 45 s pelas 9 estimativas) e custo sobe cerca de 20 ms | `netem`: 1,0 Mbit/s, custo 31 ms. `tbf` com burst de 3000 bytes: 1,0 Mbit/s e custo 22 ms, mas numa corrida 202 Mbit/s | Se a capacidade sair muito alta, o limitador deixou passar o par numa rajada: ver `tc qdisc show` no nó |
| 5 | Matar um nó | Com `kill -9` ou `sair`: os vizinhos detetam de imediato, porque o TCP fecha ou chega um LEAVE. Sem fecho de TCP (link cortado ou `kill -STOP`): detetam em cerca de 3 s, com o motivo "sem resposta aos probes" | `kill -9`: 4 ms. `kill -STOP`: 3,05 s | Para testar os 3 s, usar `kill -STOP` ou cortar o link, não `kill -9` |
| 6 | Isolar um nó: matar todos os seus vizinhos | O nó mede os candidatos e liga-se ao melhor; `vizinhos` mostra um vizinho "dinâmico". Os outros nós que perderam vizinhos, mas não ficaram isolados, não criam arestas ("sem aresta nova" no log) | 3 ms; 500 ms com um candidato morto | Sem candidatos (o único vizinho não tinha outros vizinhos), o nó volta ao bootstrapper |

Para ver a reparação no log: `grep -E "isolado|reparação|sem aresta nova" logs/<ID>.log`

## Estado da Fase 2

Funciona (testado com `python3 -m unittest discover -s tests` e com o cenário 2 em local; ainda não testado no CORE). Explicação completa em `docs/fase2.md`.
- Probes UDP com SRTT, RTTVAR e perda por link; capacidade por packet pair; função de custo com os modos `full`, `latency` e `hops`.
- Deteção de falhas também sem fecho da ligação TCP (3 s sem resposta aos probes).
- Gossip de vizinhos e autorreparação: só se cria uma aresta nova quando um nó fica isolado.
- Comunicação de controlo atrás de uma camada abstrata (`ott/transport/`), com TCP como implementação atual.

## Estado da Fase 1

Funciona (testado com `python3 -m unittest discover tests` e com os cenários 1 e 2 em local):
- Bootstrapper: lê e valida a topologia, responde a REGISTER com os vizinhos (ou ERROR para nós desconhecidos) e não intervém depois do arranque.
- Arranque dos nós em qualquer ordem, incluindo antes do bootstrapper (novas tentativas de registo a cada 2 s) e antes dos vizinhos (novas tentativas de ligação a cada 5 s).
- Ligações de controlo TCP com HELLO / HELLO_ACK / HELLO_REJECT ("full" acima de X_MAX vizinhos, "duplicate" para ligações repetidas); ligações simultâneas resolvidas pela regra do ID menor.
- Saída limpa (`sair`, Ctrl+C) com LEAVE; falha de um nó (crash) detetada pelos vizinhos através do fecho da ligação TCP; religação automática quando o nó volta.
- Fila de envio e thread escritora por vizinho; despacho das mensagens recebidas para handlers registados.

Fica para as fases seguintes:
- Fase 3: ANNOUNCE, tabela de candidatos e PING.
- Fase 4: ACTIVATE/DEACTIVATE, tabela de fluxos e plano de dados com RPF.
- Fase 5: papéis servidor e cliente (`--server`, `--streams`, `--client` já são aceites, mas ainda não fazem nada).
- Fase 6: failover rápido, make-before-break e largura de banda passiva. Opcionais: otimização da árvore e controlo em UDP.
- Topologias do CORE em `topologias/` e IPs reais em `configs/core_*.json`.
