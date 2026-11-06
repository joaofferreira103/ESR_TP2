"""Parâmetros do overlay. Todos os valores configuráveis vivem aqui (nenhum número mágico no resto do código).

Unidades: tempos em segundos, salvo indicação em contrário; custos em milissegundos.
"""

# --- Protocolo ---
ANY_IP = "0.0.0.0"                # endereço de bind em todas as interfaces (por omissão, no CORE)
PROTOCOL_VERSION = 1             # versão incluída em todas as mensagens (campo "v" / byte de versão)
MAX_CONTROL_MSG = 1024 * 1024    # tamanho máximo de uma mensagem de controlo, em bytes (1 MB)
TTL = 16                         # TTL inicial dos pacotes de dados, em saltos
CONTROL_TRANSPORT = "tcp"        # implementação do transporte de controlo entre vizinhos (ott/transport/)

# --- Portas ---
BOOTSTRAP_PORT = 4999            # TCP: bootstrapper (REGISTER -> NEIGHBORS)
CONTROL_PORT = 5000              # TCP: controlo entre vizinhos
DATA_PORT = 5001                 # UDP: plano de dados
PROBE_PORT = 5002                # UDP: probes (métricas e heartbeat)

# --- Arranque e vizinhos ---
BOOTSTRAP_RETRY_INTERVAL = 2.0   # s: intervalo entre tentativas de contactar o bootstrapper
BOOTSTRAP_REQUEST_TIMEOUT = 5.0  # s: tempo máximo que o bootstrapper espera pelo REGISTER numa ligação
ACCEPT_RETRY_DELAY = 0.1         # s: espera depois de um erro em accept() (ex.: sem descritores livres)
HELLO_TIMEOUT = 3.0              # s: tempo máximo à espera de HELLO_ACK / HELLO_REJECT
LEAVE_FLUSH_TIMEOUT = 1.0        # s: tempo máximo à espera que os LEAVE saiam antes de fechar as ligações
RECONNECT_INTERVAL = 5.0         # s: intervalo entre tentativas de ligar a vizinhos estáticos em falta
X_MAX = 4                        # nº máximo de vizinhos (acima disto, HELLO_REJECT)
SEND_QUEUE_MAX = 1000            # mensagens: tamanho da fila de envio TCP por vizinho (cheia = fecha a ligação)

# --- Métricas ---
PROBE_INTERVAL = 0.2             # s: intervalo entre probes enviados a cada vizinho
PROBE_MAX_SIZE = 2048            # bytes: tamanho máximo de um datagrama de probe recebido
PROBE_RECV_TIMEOUT = 0.5         # s: espera máxima da thread de receção antes de ver se deve parar
RTT_ALPHA = 0.125                # peso da nova medição na EWMA do RTT (adimensional)
RTTVAR_BETA = 0.25               # peso da nova medição na EWMA do desvio do RTT (adimensional)
LOSS_WINDOW = 50                 # probes: janela usada para calcular a taxa de perda
DEAD_TIMEOUT = 3.0               # s: sem resposta a probes durante este tempo = vizinho morto
K_JITTER = 1                     # peso do jitter (RTTVAR) no custo do link (adimensional)
K_LOSS_MS = 100                  # ms: penalização por perda total no custo do link (10% de perda = +10 ms)
HOP_COST = 1.0                   # ms: custo fixo somado por salto (desempate por nº de saltos)
MIN_SAMPLES = 5                  # amostras de RTT necessárias antes de usar o custo medido
DEFAULT_LINK_COST = 50.0         # ms: custo conservador de um link ainda sem amostras suficientes
COST_MODE = "full"               # função de custo: "full" (completo), só latência ou só saltos

# --- Largura de banda ---
BW_THRESHOLD = 0.8               # fração do débito esperado abaixo da qual o link é penalizado
BW_WINDOW = 3.0                  # s: tempo abaixo do limiar antes de penalizar
W_BW_MS = 10                     # ms: penalização quando a capacidade é igual ao necessário (capacidade igual ao necessário = +10 ms)
BW_REQUIRED_MBPS = 2             # Mbps: largura de banda necessária usada no custo do link
PAIR_INTERVAL = 5.0              # s: intervalo entre packet pairs enviados a cada vizinho
PAIR_SIZE = 1200                 # bytes: tamanho total de cada probe do packet pair
PAIR_HISTORY = 9                 # estimativas: janela usada para calcular a mediana da capacidade

# --- Autorreparação ---
GOSSIP_INTERVAL = 5.0            # s: período de envio da NEIGHBOR_LIST a cada vizinho
CANDIDATE_EXPIRY = 60.0          # s: candidato (vizinho de um vizinho) sem atualização durante este tempo é removido
REPAIR_PROBES = 3                # probes: nº de probes usados para medir o RTT a cada candidato
REPAIR_TIMEOUT = 0.5             # s: espera máxima pelas respostas de cada candidato
REPAIR_RETRY_INTERVAL = 5.0       # s: intervalo entre tentativas de reparação enquanto o nó estiver isolado

# --- Encaminhamento ---
ANNOUNCE_INTERVAL = 2.0          # s: período dos ANNOUNCE gerados por cada servidor
SERVER_EXPIRY = 6.0              # s: candidato sem atualização durante este tempo é removido
STABILIZATION_WINDOW = 0.2       # s: espera após um (epoch, seq) novo antes de mudar uma rota ativa
HYSTERESIS = 0.2                 # melhoria relativa mínima para mudar uma rota ativa (20%)
HYSTERESIS_ABS = 2.0             # ms: melhoria absoluta mínima para mudar uma rota ativa
TREE_MARGIN = 0.3                # Extra adiado: custo extra relativo aceite para juntar a um ramo já ativo (30%)

# --- Ativação (soft state) ---
REFRESH_INTERVAL = 2.0           # s: período de renovação dos ACTIVATE enviados ao upstream
DEST_EXPIRY = 6.0                # s: destino sem ACTIVATE durante este tempo é removido

# --- Cliente ---
CLIENT_BUFFER_FRAMES = 5         # quadros: tamanho do buffer de reprodução (~250 ms a 20 fps)

# --- Diagnóstico ---
STATE_DUMP_INTERVAL = 10.0       # s: período de escrita do estado completo do nó no log
