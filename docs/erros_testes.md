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

## Testes ainda por fazer (sem resultados nestes logs)

| Teste | Comando ou configuração |
|---|---|
| Jitter | 5 ms de jitter no link n13–n22, ver `cmd O8 metricas` |
| Perda | 10% de perda no link n12–n19, ver `cmd O10 metricas` |
| Largura de banda | 1 Mbps no link n12–n19, esperar até 45 s |
| Crash com fecho de TCP | `sudo scripts/core.sh crash O6` |
| Saída limpa | `sudo scripts/core.sh stop O3` |

Antes de os repetir: retirar o atraso de 200 ms que ficou no link n13–n22 e fazer `sudo scripts/core.sh stop` seguido de `start`, para os logs ficarem limpos.
