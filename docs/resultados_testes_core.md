# Resultados dos testes no CORE

Testes da Fase 2 feitos no CORE em 2026-11-16 e 2026-11-17, com `topologias/topologia_ott.xml` e `configs/core_ott.json`, arrancados com `sudo scripts/core.sh start`. Os logs de cada teste estão em `logs_T<n>/`. Os erros encontrados, com causa e proposta de correção, estão em `erros_testes.md` (erros 1 a 7).

Os testes unitários (`python3 -m unittest discover -s tests`) passam todos na VM. No macOS, 25 deles dão `Errno 55`, porque o macOS só tem o 127.0.0.1 como loopback. Não é um bug do código.

## Resumo

| Teste | Configuração | Resultado | Estado |
|---|---|---|---|
| T1 Arranque | nenhuma | Os 10 nós registados em cerca de 1 s, todos com os vizinhos do ficheiro, 10 min sem nenhuma falha | OK |
| T2 Base | nenhuma | SRTT de 0,35 a 1 ms, perda 0%, custo de 1,5 a 2,7 ms. Capacidade de 30 a 580 Mbit/s, sem significado (nota 1) | OK |
| T3 Atraso | 20 ms em n13–n22 | SRTT O8–O4 de cerca de 0,5 para cerca de 41 ms; custo de cerca de 1,6 para cerca de 22 ms; estável em menos de 10 s | OK |
| T4 Jitter | 20 ms + 5 ms de jitter em n13–n22 | RTTVAR de 3 a 4,5 ms (correto); **capacidade falsa** de cerca de 3 Mbit/s | Erro 5 |
| T5 Perda | 10% em n12–n19 | Perda real de cerca de 20% (dois sentidos), mas a mostrada **oscila entre 6 e 22%**; custo entre 9 e 24 ms | Erro 6 |
| T6 Largura de banda | 1 Mbps em n12–n19 | Capacidade de **1,0 Mbit/s** nos dois sentidos, ao fim de cerca de 25 s; custo de cerca de +20 ms. O CORE usa `netem rate` | OK |
| T7 Saída limpa | `stop O3` | LEAVE detetado em 2 a 3 ms; "sem aresta nova"; o O3 volta a ligar-se depois do `start` | OK |
| T8 Crash | `crash O6` | Deteção em cerca de 1 ms; O7 → O4, O10 → O7. Ilha evitada por sorte; **o O6 não consegue voltar a ligar-se ao O4 (cheio)** | Erros 1 e 3 |
| T9 Um nó isolado | `crash O4` | O8 → O5 (dinâmica) em 6 ms; os outros, "sem aresta nova". **O O4 não consegue voltar a ligar-se ao O5 (cheio)**; a aresta dinâmica fica | Erro 3 |
| T10 Falha silenciosa | `freeze O6` / `unfreeze` | Deteção em cerca de 3 s ✓. No `unfreeze`, tempestade (30 ligações perdidas em 1 s), reparação falsa e O5–O6 bloqueada até ao fim | Erros 2, 3 e 4 |

**Estado final depois de T7 a T10:** falta a aresta estática O5–O6, e existem duas arestas que não estão no ficheiro: O7–O10 e O8–O5.

## Notas

1. **Sem limite de largura de banda, a capacidade não tem significado.** A topologia tem `bandwidth="0"` em todos os links, e o packet pair mede o ritmo da VM (30 a 580 Mbit/s, sem simetria). O T6 mostrou que, com um limite real, a medida é exata. **Proposta:** uma cópia `topologia_ott_bw.xml` com 10 Mbps no backbone e nos acessos, e 2 a 3 Mbps em 1 ou 2 links, para a Fase 3 ter gargalos reais.

2. **O CORE aplica o atraso e a perda nos dois sentidos.** Um "Delay" de 20 ms dá cerca de 40 ms de RTT (T3), e 10% de perda dá cerca de 19% nos probes (T5).

3. **O CORE limita a largura de banda com `netem rate`** (`tc qdisc show` no n19: `netem ... rate 1Mbit`). Não deixa passar rajadas, ao contrário do `tbf` usado em local, que numa corrida deu 202 Mbit/s.

4. **Tempos de deteção de falhas:** LEAVE em 2 a 3 ms, crash (fecho de TCP) em cerca de 1 ms, falha silenciosa em cerca de 3 s (`DEAD_TIMEOUT`). A reparação de um nó isolado demora cerca de 6 ms.

## Correção feita: arranque no `core.sh`

Às vezes o `start` falhava em todos os nós, sem `.out` nem PID. O processo em background ficava na sessão do `sh` lançado pelo `vcmd` e era morto quando esse `sh` terminava, antes de chegar ao python. Agora:
- arranca com `setsid`, numa sessão própria;
- o `.out` é criado antes, para guardar os erros do lançamento;
- o script espera até 3 s pelo processo, em vez de 0,5 s fixos.
