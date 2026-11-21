# Erros detetados nos testes

Registo dos problemas encontrados ao testar o overlay. Para cada um: a condição de teste que o provocou, o que se observou, a causa e uma proposta de correção.

---

## Ambiente de teste

| | |
|---|---|
| Data | 2026-11-12, testes da Fase 2 no CORE |
| Topologia CORE | `topologias/topologia_ott.xml` (10 routers com OSPF, 5 switches, 1 servidor, 9 clientes) |
| Overlay | `configs/core_ott.json`: 10 nós, bootstrapper no n25 |
| Arranque | `sudo scripts/core.sh start` (todos os nós arrancados em cerca de 5 s) |
| Logs analisados | `logs/O*.log`, das 16:01 às 16:06 |

Vizinhos relevantes para os erros abaixo:

| Nó | Nó CORE | Vizinhos (ficheiro) |
|---|---|---|
| O4 | n7 | O2, O5, O6, O8 |
| O5 | n5 | O3, O4, O6, O9 |
| O6 | n2 | O4, O5, O7, O10 |
| O7 | n17 | O6 |
| O10 | n19 | O6 |

O7 e O10 dependem os dois **só** de O6.

---

## Erro 1: a reparação cria uma ilha desligada do resto do overlay

**Gravidade:** alta. Na Fase 5, os clientes da ilha ficam sem vídeo enquanto ela existir.

**Corrigido em `590e68d` e `5188e6a`:** a `NEIGHBOR_LIST` leva o grau de cada vizinho (`{id, ip, degree}`) e é reenviada logo que o grau de um vizinho muda. Na reparação, um candidato com grau <= 1 indicado só por vizinhos que já morreram fica para o fim e só é usado se nenhum outro aceitar. A questão relacionada (arestas dinâmicas) ficou resolvida com o erro 3.

### Condição de teste
Falha silenciosa de um nó que é o único vizinho de **dois ou mais** nós:
```bash
sudo scripts/core.sh freeze O6     # kill -STOP: o processo pára sem fechar as ligações TCP
```
Com esta topologia, O7 e O10 ficam os dois isolados ao mesmo tempo.

### O que se observou
O O10 detetou a falha primeiro, escolheu o O7 como melhor candidato e ligou-se a ele. Cerca de 200 ms depois, o O7 detetou a mesma falha. Como já tinha o O10 como vizinho, não se considerou isolado e não fez nada.

```
16:04:45.039 [O10] WARNING ligação a O6 perdida (sem resposta aos probes)
16:04:45.041 [O10] WARNING nó isolado: a iniciar a reparação
16:04:45.042 [O10] INFO reparação: 3 candidatos a medir (O4, O5, O7)
16:04:45.045 [O10] INFO reparação: HELLO a O7 (10.0.20.10), RTT 0.37 ms
16:04:45.051 [O10] INFO reparação concluída: ligado a O7
16:04:45.240 [O7] WARNING ligação a O6 perdida (sem resposta aos probes)
16:04:45.243 [O7] INFO perdi O6; vizinhos restantes: 1; sem aresta nova
```

Resultado: **{O7, O10} formam uma componente separada do overlay**, sem nenhum caminho até ao O1 (servidor). A situação manteve-se cerca de 45 s, até o O6 voltar.

### Causa
- A reparação ordena os candidatos só pelo RTT. O O7 tinha o RTT mais baixo (0,37 ms), mas o seu único vizinho era o próprio O6, o nó que falhou.
- A regra "só se cria uma aresta nova quando o nó fica isolado" (secção 5 do `fase2.md`) é avaliada por cada nó de forma independente. Dois nós isolados que se ligam um ao outro deixam de estar isolados, embora continuem desligados da rede.
- O resultado depende da ordem de deteção: se o O7 tivesse detetado primeiro, ter-se-ia ligado ao O10 (ou ao O4/O5) e o resultado podia ser diferente.

### Correção proposta
1. **Excluir os candidatos cuja lista de vizinhos conhecida só tem o nó que falhou** (ou, de forma mais geral, os que foram indicados apenas pelo vizinho que falhou e não têm outros vizinhos). Com isto, o O10 teria escolhido o O4 ou o O5.
2. Em alternativa, dar menor prioridade a esses candidatos em vez de os excluir: usam-se só se não houver mais nenhum.
3. A deteção completa da partição (o nó deixa de receber `ANNOUNCE` do servidor) só fica possível na Fase 3. Pode servir de segunda proteção: quem deixa de ter rota para o servidor volta a tentar a reparação.

### Questão relacionada (para decidir em grupo)
Depois de o O6 voltar, a aresta **dinâmica** O7–O10 continua ativa (O7 e O10 ficam com 2 vizinhos cada). Deve ser fechada quando os vizinhos estáticos voltam, para a topologia regressar à do ficheiro? Se ficar, a topologia vai acumulando arestas dinâmicas a cada falha.

```
16:06:16.241 [O7] INFO Estado de O7: 2 vizinhos (2 up), 1 estáticos
  O10 10.0.21.10 up dinâmico iniciada pelo vizinho há 91s fila=0
  O6 10.0.20.1 up estático iniciada pelo vizinho há 46s fila=0
```

---

## Erro 2: tempestade de ligações quando um nó parado volta

**Gravidade:** média. Agora acaba por estabilizar, mas na Fase 3 cada `up` e `down` vai provocar novos anúncios e mudanças de rota.

**Corrigido em `d54c608`:** handshake em 3 passos (`HELLO` -> `HELLO_ACK` -> `HELLO_CONFIRM`). Quem aceita só marca o vizinho `up` ao receber o `HELLO_CONFIRM`, no máximo `HELLO_TIMEOUT` depois; uma ligação que fecha ou expira antes disso é descartada em silêncio (DEBUG), sem `up` nem `down`. Um HELLO novo do mesmo vizinho substitui o que estava pendente. `listen()` com `LISTEN_BACKLOG = 8`. Em local, com O3 congelado 20 s, os `down` de O3 passaram de 11 para 3.

**Corrigido também em `1f41b6d` (pausa do próprio nó):** se a verificação de vizinhos mortos chega com mais de `PAUSE_DETECT_SLACK` (1 s) de atraso, o nó não declara ninguém morto nessa ronda e dá a todos um novo `DEAD_TIMEOUT` (log "pausa local de X s detetada"). Com toda a VM parada 5 s, os `down` passaram de 12 para 0 e as reparações falsas de 6 para 0. Com um só nó congelado 20 s restam 4 `down`: 3 por "ligação fechada", porque os vizinhos, que não pararam, o declararam morto e fecharam as ligações (correto), e 1 de uma aresta dinâmica criada pela reparação e desfeita em menos de 0,5 s, quando o primeiro vizinho estático voltou.

### Condição de teste
Um nó parado durante vários segundos enquanto os vizinhos tentam ligar-se a ele, e que depois volta:
```bash
sudo scripts/core.sh freeze O6     # 16:04:42
# ~45 s depois
sudo scripts/core.sh unfreeze O6   # 16:05:30
```

### O que se observou
No primeiro segundo depois do `unfreeze`, o log do O6 mostra **35 ligações aceites, 37 ligações perdidas e 74 eventos `up` ou `down`**, quase todos em 80 ms. Há HELLOs rejeitados como duplicados, erros de escrita e contagens de vizinhos que sobem e descem:

```
16:05:30.003 [O6] WARNING ligação a O5 perdida (ligação fechada)
16:05:30.009 [O6] DEBUG evento: vizinho O5 up
16:05:30.012 [O6] DEBUG evento: vizinho O5 down (ligação fechada)
16:05:30.018 [O6] WARNING ligação a O4 perdida (erro de escrita)
16:05:30.025 [O6] INFO perdi O5; vizinhos restantes: 1; sem aresta nova
16:05:30.049 [O6] INFO HELLO de O10 rejeitado (duplicate)
...
16:05:40.087 [O6] INFO Estado de O6: 4 vizinhos (4 up), 4 estáticos
```

Às 16:05:40 o O6 tinha os 4 vizinhos ativos e todos os outros nós o viam como "up", ou seja, o estado final ficou correto.

### Causa
1. Enquanto o O6 estava parado, o O4, o O5, o O7 e o O10 continuaram a tentar ligar-se a ele a cada 5 s.
2. O kernel do n2 continua a funcionar com o processo parado: completa o handshake TCP e deixa as ligações na fila do `listen()`, à espera de um `accept()` que não chega. Quando a fila enche, as tentativas seguintes falham com `timed out`, o que aparece nos logs dos vizinhos.
3. Os vizinhos cujas ligações tinham entrado na fila enviaram HELLO, não receberam `HELLO_ACK` em 3 s (`HELLO_TIMEOUT`) e desistiram, fechando a ligação do lado deles.
4. Quando o O6 volta, aceita de uma vez todas as ligações da fila. Muitas já estão fechadas do outro lado, mas cada uma gera um `up` seguido de um `down` (`ligação fechada` ou `erro de escrita`). Algumas chegam enquanto já há uma ligação ativa ao mesmo vizinho e são rejeitadas como `duplicate`.

### Correção proposta
1. **Não gerar o evento `up` antes de o HELLO estar completo dos dois lados.** Uma ligação que fecha antes disso deve ser descartada em silêncio, sem `up`, `down` nem "perdi X".
2. Quando se aceita uma ligação, descartar os HELLO com mais de `HELLO_TIMEOUT` (o emissor já desistiu). Para isso o HELLO tem de levar o instante de envio, ou um número de tentativa.
3. Opcional: limitar a fila do `listen()` (por exemplo `listen(4)`) para reduzir o número de ligações velhas acumuladas.

---

# Erros detetados na segunda ronda (2026-11-16 e 2026-11-17)

Testes T1 a T10 de `resultados_testes_core.md`. Logs em `logs_T<n>/` (os erros 3 e 4 estão em `logs_T7a10/`).

**Confirmação dos erros 1 e 2:**
- **Erro 1:** no T8 (crash do O6), o O10 escolheu outra vez o O7 pelo RTT mais baixo. A ilha só não se formou porque o O7 detetou a falha 2 ms antes e se ligou ao O4. A causa continua no código.
- **Erro 2:** no T10, ao fazer `unfreeze`, o O6 teve 30 ligações perdidas e 4 HELLO rejeitados como `duplicate` em cerca de 1 s.

---

## Erro 3: as arestas dinâmicas ocupam o lugar dos vizinhos estáticos

**Gravidade:** alta. A topologia do ficheiro deixa de se repor. A cada falha acumulam-se arestas dinâmicas, e algumas arestas estáticas ficam bloqueadas sem limite de tempo.

**Corrigido em `d7bb43d`:** a aresta criada pela reparação leva `"dynamic": true` no HELLO e fica marcada nos dois lados. (a) Quem a criou fecha-a com LEAVE quando volta a ter um vizinho estático `up`. (b) Um nó cheio que recebe HELLO de um vizinho estático fecha uma aresta dinâmica para lhe dar lugar, se o outro lado tiver grau > 1 segundo o gossip; senão responde `full`.

**Ajustado em `ad022c0`:** a vaga só é cedida ao receber o `HELLO_CONFIRM` do vizinho estático. Durante o handshake o nó pode ter `X_MAX + 1` ligações; se o CONFIRM não chegar, a aresta dinâmica mantém-se.

### Condição de teste
Uma reparação liga um nó isolado a um candidato que fica com `X_MAX` (4) vizinhos. Depois volta um vizinho estático desse candidato:
```bash
sudo scripts/core.sh crash O4      # O8 isolado liga-se ao O5: O5 = O3, O6, O9, O8 (dinâmica)
sudo scripts/core.sh start O4      # o O4 quer voltar a ligar-se ao O5, que está cheio
```

### O que se observou
| Teste | Cheio | Por causa de | Bloqueou | Durante |
|---|---|---|---|---|
| T8 | O4 (O2, O5, O8, **O7 dinâmica**) | reparação do O7 | O6 → O4 | 61 s, até o O4 ir abaixo |
| T9 | O5 (O3, O6, O9, **O8 dinâmica**) | reparação do O8 | O4 → O5 | 62 s, até o O6 congelar |
| T10 | O5 (O3, O4, O9, **O8 dinâmica**) | a mesma | O6 → O5 | até ao fim dos logs |

```
16:45:00.351 [O5] INFO HELLO de O4 rejeitado (full)
16:45:05.360 [O5] INFO HELLO de O4 rejeitado (full)
... (a cada 5 s)
16:47:12.035 [O6] INFO Estado de O6: 3 vizinhos (3 up), 4 estáticos
```
Nas duas primeiras vezes, o bloqueio só acabou porque outra falha libertou uma vaga. No fim, falta a aresta estática O5–O6, e existem duas arestas que não estão no ficheiro: O7–O10 e O8–O5.

### Causa
- `X_MAX` conta todos os vizinhos por igual. Uma aresta dinâmica criada pela reparação fica com uma vaga que pertence a um vizinho do ficheiro.
- As arestas dinâmicas nunca são fechadas, nem quando os vizinhos estáticos voltam. É a questão em aberto do erro 1.

### Correção proposta
1. **Os vizinhos estáticos têm prioridade.** Quando chega um HELLO de um vizinho estático e o nó está cheio, fecha-se uma aresta dinâmica para lhe dar lugar, desde que o outro lado dessa aresta tenha mais vizinhos (para não o isolar).
2. **Fechar as arestas dinâmicas quando deixam de ser precisas:** um nó com uma aresta dinâmica fecha-a quando um vizinho estático seu volta a estar `up`.
3. Em alternativa, dar `X_MAX` só aos estáticos e um limite à parte para as dinâmicas (por exemplo +1). É mais simples, mas o grau dos nós cresce.

---

## Erro 4: reparação falsa durante a tempestade, abandonada sem nada no log

**Gravidade:** baixa. É uma consequência do erro 2 e fica resolvido quando este for corrigido.

**Corrigido em `d54c608`:** com o handshake em 3 passos já não há `up`/`down` falsos; e a reparação cancelada escreve "reparação cancelada: <vizinho> voltou".

### O que se observou (T10)
No `unfreeze`, a contagem de vizinhos do O6 chegou a 0 a meio da tempestade. O O6 julgou-se isolado e começou uma reparação com 7 candidatos. Os vizinhos voltaram e a reparação parou sem escrever nada:
```
16:46:51.986 [O6] WARNING nó isolado: a iniciar a reparação
16:46:52.007 [O6] INFO reparação: 7 candidatos a medir (O10, O2, O3, O5, O7, O8, O9)
(sem mais nenhuma linha da reparação)
```

### Causa e correção
- Os `up` e `down` falsos do erro 2 levam a contagem de vizinhos a 0.
- `repair()` em `ott/repair.py` faz `return` em silêncio quando `_no_longer_needed()` é verdadeiro. Deve escrever, por exemplo, "reparação cancelada: vizinho X voltou".

---

## Erro 5: com jitter, o packet pair mede capacidade falsa

**Gravidade:** média. Põe custo a mais num link e faz o mesmo link ter custos diferentes em cada ponta.

- **Condição (T4):** 20 ms de atraso + 5 ms de jitter no link n13–n22, num link **sem** limite de largura de banda.
- **Observado:** O8→O4 com 2,9 a 4,3 Mbit/s; O4→O8 a descer de 58 para 7,7 Mbit/s. Cerca de +6,7 ms de custo vêm só da capacidade falsa.
- **Causa:** o netem atrasa e troca a ordem dos pacotes de forma aleatória. O intervalo entre os dois pacotes do par passa a ser ruído de alguns ms, e 1200 B em cerca de 3 ms dão cerca de 3 Mbit/s.
- **Correção:** descartar as estimativas em que o intervalo entre os pacotes do par é comparável ao RTTVAR, ou ignorar o termo de capacidade quando o RTTVAR é alto.
- **Corrigido em `be09655`:** um par com dispersão menor que `PAIR_RTTVAR_FACTOR` (2) × RTTVAR é descartado. Limitação em `fase2.md`, secção 4.

---

## Erro 6: a estimativa da perda oscila muito

**Gravidade:** média para a Fase 3, porque faz as rotas mudar sem razão.

- **Condição (T5):** 10% de perda no link n12–n19. O CORE aplica-a nos dois sentidos, o que dá cerca de 19%.
- **Observado:** a perda real ficou em cerca de 20%: chegaram 36 a 45 respostas por cada 50 probes. Mas a perda mostrada variou entre **6,4% e 21,7%**, e o custo entre 9 e 24 ms.
- **Causa:** a janela tem 50 probes, mas os dos últimos 3 s (`DEAD_TIMEOUT`) não contam, o que deixa cerca de 35. Isso dá um desvio padrão de cerca de ±7 pontos.
- **Correção:** uma janela maior (por exemplo 150 probes, cerca de 30 s) ou uma média móvel da perda, como no SRTT. Na Fase 3, usar histerese na troca de rota.
- **Corrigido em `36a98a7`:** só entram na perda os probes com mais de `LOSS_EVAL_DELAY` (1 s), respondidos ou não (antes os respondidos recentes contavam e os perdidos recentes não, o que subestimava a perda); `LOSS_WINDOW` passou de 50 para 100. Com 20% de perda uniforme simulada, a estimativa fica em 20% ± 5 pontos em todas as janelas. A histerese fica para a Fase 3.

---

## Erro 7: o custo reage muito ao RTTVAR quando a VM para

**Gravidade:** média para a Fase 3.

- **Observado (T1 às 19:46, T4 às 20:00:06):** picos em vários links ao mesmo tempo, sem perda e com cerca de 6 amostras em falta, sinal de que a VM parou cerca de 1 s. Um link com SRTT de 0,5 ms chegou a um custo de **91 ms**, com RTTVAR de 72 ms.
- **Causa:** `K_JITTER = 1` soma o RTTVAR inteiro ao custo, e um único RTT muito alto faz o RTTVAR disparar.
- **Correção:** limitar o termo do jitter (por exemplo, no máximo igual ao SRTT), suavizar o custo e usar histerese na troca de rota.
- **Corrigido em `9242567`:** o termo do jitter passa a ser `min(K_JITTER × RTTVAR, JITTER_CAP_MS)`, com `JITTER_CAP_MS = 20`. O caso do CORE (SRTT 0,5 ms, RTTVAR 72 ms) passa de 73 para 21 ms. A histerese fica para a Fase 3.
