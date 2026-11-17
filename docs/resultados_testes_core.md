# Resultados dos testes no CORE

Testes da Fase 2 feitos no CORE em 2026-11-16, com `topologias/topologia_ott.xml` e `configs/core_ott.json`, arrancados com `sudo scripts/core.sh start`. Os logs de cada teste estão em `logs_T<n>/`. Os erros já conhecidos (ilha na reparação, tempestade de ligações) estão em `erros_testes.md`.

Os testes unitários (`python3 -m unittest discover -s tests`) passam todos na VM. No macOS, 25 deles dão `Errno 55`, porque o macOS só tem o 127.0.0.1 como loopback. Não é um bug do código.

## Resumo

| Teste | Configuração | Resultado | Estado |
|---|---|---|---|
| T1 Arranque | nenhuma | Os 10 nós registados em cerca de 1 s, todos com os vizinhos do ficheiro, 10 min sem nenhuma falha | OK |
| T2 Base | nenhuma | SRTT de 0,35 a 1 ms, perda 0%, custo de 1,5 a 2,7 ms. Capacidade de 30 a 580 Mbit/s, sem significado (ver nota 1) | OK |
| T3 Atraso | 20 ms em n13–n22 | SRTT O8–O4 de cerca de 0,5 para cerca de 41 ms; custo de cerca de 1,6 para cerca de 22 ms; estável em menos de 10 s | OK |
| T4 Jitter | 20 ms + 5 ms de jitter em n13–n22 | RTTVAR de 3 a 4,5 ms (correto); **capacidade falsa** de cerca de 3 Mbit/s | Problema (nota 2) |
| T5 Perda | 10% em n12–n19 | | por fazer |
| T6 Largura de banda | 1 Mbps em n12–n19 | | por fazer |
| T7 a T10 Falhas | ver o plano no README | | por fazer |

## Notas

1. **Sem limite de largura de banda, a capacidade não tem significado.** A topologia tem `bandwidth="0"` em todos os links. O packet pair mede o ritmo da VM (30 a 580 Mbit/s, e nem sequer é simétrico). Esse ruído chega a mudar o custo em cerca de 0,6 ms, o mesmo que o SRTT de base. Proposta: uma cópia `topologia_ott_bw.xml` com 10 Mbps no backbone e nos acessos, e 2 a 3 Mbps em 1 ou 2 links, para haver gargalos reais. Só depois do T6 confirmar que o packet pair mede bem um limite no CORE.

2. **O jitter estraga a estimativa de capacidade (T4).** O netem atrasa e troca a ordem dos pacotes de forma aleatória, e o intervalo entre os dois pacotes do par passa a ser ruído de alguns ms: 1200 B em cerca de 3 ms dão cerca de 3 Mbit/s. O O8→O4 deu de 2,9 a 4,3 Mbit/s. O O4→O8 foi descendo de 58 para 7,7 Mbit/s à medida que a mediana de 9 se enchia de valores contaminados. Dos cerca de +10 ms de custo em relação ao T3, só cerca de 4 vêm do jitter. Os outros cerca de 6,7 vêm da capacidade falsa (`10 × 2 / 3`). O mesmo link fica também com custos diferentes em cada ponta (31 contra 28 ms). Correção possível: descartar as estimativas em que o intervalo entre os pacotes do par é comparável ao RTTVAR, ou ignorar o termo de capacidade quando o RTTVAR é alto.

3. **O CORE aplica o atraso nos dois sentidos (T3).** Um "Delay" de 20 ms no link dá cerca de 40 ms de RTT. Para a perda, espera-se então cerca de 19% (1 − 0,9²) com 10% configurados.

4. **Há picos quando a VM para (T1 às 19:46, T4 às 20:00:06).** Vários links ao mesmo tempo, sem perda e com amostras em falta, sinal de que a VM parou cerca de 1 s. No pior caso, um link com 0,5 ms de SRTT chegou a um custo de 91 ms, porque o RTTVAR foi a 72 ms. **O custo reage muito ao RTTVAR:** na Fase 3 isto pode fazer as rotas mudar sem razão. Vale a pena suavizar (por exemplo, limitar o termo do jitter ou usar histerese na troca de rota).

## Correção feita: arranque no `core.sh`

Às vezes o `start` falhava em todos os nós, sem `.out` nem PID. O processo em background ficava na sessão do `sh` lançado pelo `vcmd` e era morto quando esse `sh` terminava, antes de chegar ao python. Agora:
- arranca com `setsid`, numa sessão própria;
- o `.out` é criado antes, para guardar os erros do lançamento;
- o script espera até 3 s pelo processo, em vez de 0,5 s fixos.
