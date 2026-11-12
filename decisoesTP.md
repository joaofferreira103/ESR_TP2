# Decisões de Arquitetura e Desenho — TP2 ESR (2026/2027)

> **Contexto:** Trabalho Prático Nº 2 de Engenharia de Serviços em Rede (MEI, Universidade do Minho)  
> **Referência:** [Discussão e Notas de Apoio](https://claude.ai/share/339523f4-0279-4c11-89f4-695a78b417c5)

---

## 1. Objetivo do Projeto
Construir uma rede **multicast na camada aplicacional** (*Application-Layer Multicast* / Overlay OTT): uma mini-CDN de distribuição de streaming multimédia (MJPEG/RTP) executada no emulador CORE.

---

## 2. Escolha da Linguagem: Python
- **Justificação:**
  - Código base de apoio do livro (*Kurose & Ross*) já disponibilizado e adaptado em Python.
  - Compatibilidade nativa com os contentores Linux do CORE.
  - Rapidez e flexibilidade no ciclo de desenvolvimento, teste e depuração.
  - Com o débito do MJPEG em ambiente de teste, o GIL e o desempenho do Python não constituem estrangulamento.

---

## 3. Protocolos de Transporte
Optou-se por uma arquitetura híbrida de transporte:

| Tipo de Tráfego | Protocolo | Justificação / Características |
| :--- | :---: | :--- |
| **Controlo** | **TCP** | Garante entrega fiável, ordenação e ausência de mensagens duplicadas (ex.: um `ACTIVATE` perdido deixaria o cliente sem stream). A queda da conexão TCP sinaliza falhas imediatas de vizinhos. |
| **Dados (Stream)** | **UDP** | Tráfego de tempo real sensível à latência. Perder um frame é tolerável, mas atrasos acumulados (*Head-of-Line Blocking* do TCP) degradam o fluxo. Menor *overhead* na replicação. |
| **Probes (Métricas)** | **UDP** | Avaliam RTT e taxa de perda real no mesmo canal que os dados percorrem. Se fossem em TCP, as retransmissões mascarariam as perdas da rede. |

> [!NOTE]
> O TCP demora a detetar falhas silenciosas (*crashes* sem encerramento formal de socket), pelo que os probes UDP funcionam em simultâneo como *heartbeats*.

---

## 4. Construção e Gestão da Rede Overlay

### 4.1. Arranque com Bootstrapper
1. **Papel:** Ponto de encontro (*rendezvous point*) estático inicial. Conhece a topologia pretendida através de um ficheiro de configuração manual.
2. **Ciclo de Inicialização:**
   - O nó arranca: `ott <nome_no> <ip_bootstrapper>`.
   - Envia `REGISTER {id, ip}`.
   - O bootstrapper responde com `NEIGHBORS [ip_a, ip_b, ip_c]`.
   - O nó estabelece conexões diretas com esses vizinhos (`HELLO`).
   - A partir deste ponto, o nó **nunca mais comunica com o bootstrapper** para controlo de rotas ou manutenção. O funcionamento da rede passa a ser 100% distribuído.

### 4.2. Identidade dos Nós
- **Identificação Lógica:** Cada nó recebe um identificador único por nome (ex.: `O1`, `O2`, `O5`) passado via CLI: `ott O5 <ip_bootstrapper>`.
- **Motivação:** No emulador CORE, um nó possui frequentemente múltiplas interfaces e endereços IP. Identificar nós por IP causaria conflitos e duplicações. Os nomes facilitam a leitura de tabelas, logs e diagramas.

### 4.3. Parâmetros e Gestão de Vizinhança
- **Grau de Vizinhança:**
  - $X_{\min} = 2$, $X_{\max} = 4$ (normalmente 2 a 3 vizinhos no ficheiro de configuração).
  - *Justificação:* 1 vizinho formaria uma árvore rígida (vulnerável a partições); 2 vizinhos toleram 1 falha; 3 vizinhos toleram 2 falhas. Acima de 4, o tráfego de controlo cresce sem ganho relevante para uma rede de cerca de 10 nós.
- **Deteção de Falhas (Heartbeats):**
  - Probes UDP enviados a cada **1 s**.
  - Nó considerado inativo/morto após **3 probes sem resposta** (~3 s) ou de imediato se a conexão TCP fechar.
  - A tolerância a 3 falhas evita falsos positivos perante perdas pontuais configuradas nos links.
- **Entrada a meio da execução:** Ao estabelecer conexão (`HELLO`), o vizinho envia imediatamente o último anúncio (`ANNOUNCE`) que conhece de cada servidor, integrando o nó na rota em milissegundos.
- **Saída Suave vs Falha:**
  - *Saída graciosa:* O nó envia `LEAVE` aos vizinhos antes de terminar.
  - *Falha abrupta (Crash):* Detetada pelo *timeout* dos heartbeats.
- **Controlo de Saturação de Vizinhança:** Se um nó atingir $X_{\max}$ vizinhos, responde `HELLO_REJECT` a novos pedidos de conexão, forçando o requerente a procurar outro par e evitando sobrecarga nos nós centrais.

---

## 5. Encaminhamento e Construção de Rotas

### 5.1. Anúncios Periódicos (*Flooding* Controlado pelo Servidor)
- Os servidores realizam *flooding* periódico de mensagens:
  ```json
  ANNOUNCE { "server": "S1", "streams": ["filmeA", "filmeB"], "seq": 42, "cost": 0 }
  ```
- O campo `seq` é estritamente incrementado pelo servidor a cada novo ciclo.
- **Sem envio de caminho (*path*):** Poupa largura de banda e simplifica as mensagens protocolares.

### 5.2. Regras de Aceitação ("Best Cost Wins")
Ao receber um `ANNOUNCE` do vizinho $V$, o nó calcula:
$$\text{custo\_novo} = \text{cost} + \text{custo\_link}(V)$$

Regras aplicadas:
1. **Se `seq > seq_guardado` (informação mais recente):**
   - Aceita sempre, mesmo que o custo seja superior (necessário para reagir a rotas que degradaram ou enlaces caídos).
   - Atualiza `Origem = V`, guarda `seq` e `custo`, e reencaminha a todos os vizinhos exceto $V$.
2. **Se `seq == seq_guardado` e `custo_novo < custo_guardado` (caminho mais eficiente para o mesmo anúncio):**
   - Atualiza `Origem = V` e reenvia aos vizinhos.
3. **Caso contrário:** Descarta a mensagem (cópia repetida ou pior).

> [!TIP]
> A estratégia *"Best Cost Wins"* foi preferida à *"First Copy Wins"*. A primeira cópia recebida aproxima a menor latência instantânea, mas é distorcida pelo tempo de processamento local dos nós e impede a otimização de outras métricas (perda de pacotes, saturação de largura de banda).

### 5.3. Prevenção de Ciclos e Estabilidade
- **Sem Loops:** O *flooding* termina porque um nó só reencaminha pacotes se o custo for estritamente menor para o mesmo `seq`. Como os custos de ligação são positivos, mensagens que circulem em ciclo chegam com custo superior e são descartadas.
- **Sem *Count-to-Infinity*:** Apenas a fonte (servidor) gera novos números de sequência; informação obsoleta é descartada por sequências mais recentes.
- **Janela de Estabilização e Histerese:**
  - Rotas inativas atualizam-se instantaneamente.
  - Rotas ativas mantêm-se estáveis durante uma pequena janela após um novo `seq` (~200 ms) para absorver pacotes em trânsito, só comutando se a melhoria exceder **20% de histerese** (*make-before-break*).

### 5.4. Ativação de Fluxo e Estado Temporário (*Soft State*)
- **Rotas Inativas por Padrão:** As tabelas contêm o caminho conhecido, mas o envio de dados só ocorre sob procura.
- **Pedido de Ativação:**
  - O cliente envia `ACTIVATE {fluxo}` ao nó montante indicado no campo `Origem`.
  - Cada nó intermediário regista o requerente na lista de `Destinos` daquele fluxo.
  - O nó só repassa o `ACTIVATE` para o seu próprio upstream se ainda **não** estiver a receber a referida stream.
- **Soft State:** Os clientes renovam periodicamente a ativação da rota. Destinos sem renovação expiram automaticamente (elimina dependência de rotinas de encerramento estritas e tolera falhas abruptas). O envio de `DEACTIVATE` é suportado como otimização explícita.

### 5.5. Estrutura da Tabela de Rotas
Cada nó mantém entradas com o seguinte formato:
```python
{
    "Fluxo": "S1/filmeA",
    "Servidor": "S1",
    "Seq": 42,
    "Custo": 18.5,            # Ex: ms calculados
    "Origem": "O2",           # Nó montante (upstream)
    "Destinos": ["O3", "O7"], # Vizinhos a jusante (downstream)
    "Estado": "ativa"         # ativa | inativa
}
```

---

## 6. Métricas de Rede e Qualidade de Serviço

### 6.1. Cálculo do Custo de Ligação (RTT e Perda)
Os nós estimam o custo de cada enlace com probes periódicos em UDP, calculando médias móveis exponenciais ponderadas (EWMA):
$$\text{média\_nova} = \alpha \times \text{medição} + (1 - \alpha) \times \text{média\_anterior} \quad (\alpha \approx 0.125)$$

O custo combinado de cada ligação é dado por:
$$\text{Custo do Link} = \frac{\text{RTT}_{\text{EWMA}}}{2} \times \left(1 + k \times \text{Perda}_{\text{EWMA}}\right)$$

- Usar o RTT por link evita a necessidade de sincronização horária (NTP) entre nós para medir atrasos unidirecionais.
- O número de saltos (*hop count*) serve de critério de desempate e termo de comparação no relatório.

### 6.2. Gestão de Largura de Banda
- **Eliminação de Tráfego Redundante:** Cada fluxo atravessa cada enlace no máximo uma vez, garantido pela árvore multicast com replicação nos nós intermédios.
- **Prevenção de Saturação:** Monitorização passiva do débito recebido. Como o MJPEG possui um débito constante e previsível, débitos inferiores ao esperado indicam congestionamento, penalizando o custo desse link. A largura de banda funciona como restrição e o atraso como critério de otimização.

---

## 7. Múltiplos Servidores e Descoberta de Conteúdos
- **Identificação Unívoca:** Par `(servidor, stream)` (ex.: `S1/streamA`, `S1/streamB`, `S2/streamC`).
- **Anúncios Agrupados:** Cada servidor faz um único *flooding* com a listagem das suas streams ativas. O percurso até ao servidor é partilhado entre as suas streams, mantendo `Destinos` e `Estado` independentes por fluxo.
- **Descoberta Automática:** O cliente consulta o campo `streams[]` dos anúncios recebidos para apresentar opções ao utilizador, eliminando a necessidade de servidores de catálogo centrais.

---

## 8. Formato e Codificação das Mensagens

### 8.1. Mensagens de Controlo (TCP)
- Codificação em **JSON** antecedido por um cabeçalho fixo de **4 bytes (Big Endian)** com o comprimento do payload:
```
+--------------------------+----------------------------------+
| 4 Bytes (Tamanho Payload)| Corpo JSON da Mensagem           |
+--------------------------+----------------------------------+
```
- Legível no Wireshark, auto-explicativo nos logs e fácil de estender mantendo compatibilidade via campos `tipo` e `versao`.

### 8.2. Pacotes de Dados (UDP / RTP)
- Cabeçalho binário compacto antecedendo o payload RTP:
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    Versão     |   Tipo Msg    |         ID do Fluxo           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Número de Sequência                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Timestamp                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      TTL      |                   Reservado                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Payload RTP ...                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

---

## 9. Funcionalidades Avançadas e Otimizações (Extras)

### 9.1. Autorreparação Distribuída da Rede (*Gossip*)
Quando o número de vizinhos desce abaixo de $X_{\min} = 2$ devido a falhas ou saídas (sem recorrer ao bootstrapper):
1. O nó envia `NEIGHBOR_REQ` aos vizinhos que permanecem ativos.
2. Os vizinhos respondem com `NEIGHBOR_LIST [ids e ips dos seus vizinhos]`.
3. O nó descarta duplicados e a si próprio, avalia a latência aos candidatos via *probe*, e conecta-se ao de menor RTT (`HELLO`).
4. Mantém uma *cache local* de nós conhecidos. Apenas recorre ao bootstrapper se estiver totalmente isolado.

### 9.2. Otimização da Árvore de Distribuição (Heurística de Steiner Tree)
- Cada `ANNOUNCE` reencaminhado inclui o campo `ativos: [streams que este nó já recebe]`.
- Ao ativar um fluxo, o nó prefere escolher como upstream um vizinho que **já recebe** o fluxo, desde que o custo total não exceda em **30%** o custo do melhor caminho absoluto.
- **Vantagem:** Reduz a duplicação de percursos no núcleo da rede ao fundir ramos quando os clientes estão topologicamente próximos, reduzindo a largura de banda global à custa de um acréscimo marginal de atraso.

---

## 10. Arquitetura de Software e Concorrência
Implementação em Python com arquitetura multi-threaded protegida por `threading.RLock`:

- **Divisão de Threads por Nó:**
  1. `TCP Acceptor`: Escuta e aceita novas ligações TCP.
  2. `TCP Listener (por vizinho)`: Leitura contínua e deserialização de mensagens de controlo.
  3. `UDP Data Receiver / Forwarder`: Receção de pacotes RTP e reencaminhamento imediato para os `Destinos`.
  4. `UDP Probe Manager`: Disparo periódico de probes e receção de métricas de enlace.
  5. `Timer Thread`: Gestão de anúncios periódicos, renovações *soft-state*, expiração de rotas e monitorização de *heartbeats*.
- **Disciplina de Sincronização:**
  - O lock é mantido estritamente durante a leitura/escrita atómica das estruturas de dados.
  - **Nunca efetuar I/O de rede com o lock retido** (copia-se a lista de destinos sob lock, liberta-se o lock e processa-se o envio).

---

## 11. Cliente e Métricas de Qualidade de Experiência (QoE)

### 11.1. Reprodução no Cliente
- **Buffer de Jitter:** Buffer de 5 quadros (~250 ms), amortecendo flutuações e pequenas transições de rota.
- **Descarte de Quadros Obsoletos:** Imagens que chegam fora de ordem ou atrasadas em relação ao quadro atualmente em exibição são descartadas de imediato.

### 11.2. Métricas de QoE a Medir (para Relatório e Testes)
- Taxa percentual de pacotes perdidos.
- Variação do atraso (*jitter*).
- Tempo até ao primeiro quadro (*startup delay*).
- Duração das interrupções (*freeze time*) durante falhas e transições de rotas.

---

## 12. Questões a Validar com o Docente

- [ ] **Diagnóstico com PING (Etapa 3):** O pacote de PING pode acumular o caminho percorrido (*trace*) diretamente no seu cabeçalho/payload, ou a reconstrução do trajeto deve ser realizada exclusivamente pela agregação e análise cruzada dos ficheiros de log locais?
- [ ] **Topologia do Bootstrapper:** O bootstrapper pode partilhar o mesmo nó de rede (host) que um dos servidores de streaming, ou deve obrigatoriamente residir num equipamento separado?
- [ ] **Mecanismo de Soft State:** A gestão do estado das rotas ativas através de *soft state* (renovações periódicas e expiração automática por timeout) é considerada suficiente em substituição de um encerramento explícito obrigatório (*teardown*)?

