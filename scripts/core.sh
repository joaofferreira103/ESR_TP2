#!/usr/bin/env bash
# Controla o overlay dentro de uma sessão do CORE já arrancada, a partir de UM terminal normal da VM
# (não é preciso abrir o terminal de cada nó). Corre os processos dentro dos nós com o vcmd do CORE.
#
# Uso: sudo scripts/core.sh <comando> [argumentos]
#   start [ID ...]       arranca o bootstrapper (se ainda não estiver a correr) e os nós (todos, ou só os indicados)
#   stop [ID ...]        termina os nós com "sair" (sem IDs: todos e o bootstrapper)
#   status               mostra que processos estão a correr
#   cmd <ID> <comando>   envia vizinhos|metricas|candidatos|sair ao nó e mostra a resposta
#   crash <ID>           kill -9: falha com fecho do TCP (deteção imediata)
#   freeze <ID>          kill -STOP: falha silenciosa (deteção pelos probes em cerca de 3 s)
#   unfreeze <ID>        kill -CONT
#
# Variáveis opcionais:
#   CONFIG=configs/core_ott.json   topologia; cada nó tem "core_node" (nome do nó no CORE) e,
#                                  opcionalmente, "args" (ex.: ["--server"]); "bootstrapper" diz onde corre o bootstrapper
#   SESSION=/tmp/pycore.N          sessão do CORE (por omissão, a mais recente)
#
# Saída: logs/<ID>.log (log do nó) e logs/<ID>.out (stdout/stderr, inclui as respostas aos comandos).
# Os FIFOs e PIDs ficam em <pasta acima do projeto>/.ott_run, fora da pasta partilhada (o vboxsf não suporta FIFOs).
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT=$(pwd)
CONFIG=${CONFIG:-configs/core_ott.json}
RUN_DIR="$(dirname "$PROJECT")/.ott_run"   # ex.: /home/core/.ott_run (visível na VM e em todos os nós)

[ "$(id -u)" = 0 ] || { echo "corre com sudo (o vcmd precisa de root)"; exit 1; }
command -v vcmd >/dev/null || { echo "vcmd não encontrado: isto tem de correr na VM do CORE"; exit 1; }
mkdir -p "$RUN_DIR" logs

if [ -z "${SESSION:-}" ]; then
    SESSION=$(ls -dt /tmp/pycore.* 2>/dev/null | head -n1 || true)
    [ -n "$SESSION" ] || { echo "nenhuma sessão do CORE a correr: carrega no botão verde primeiro"; exit 1; }
fi

# Linhas "ID core_node ip args...", com o bootstrapper primeiro
TABLE=$(python3 - "$CONFIG" <<'EOF'
import json, sys
data = json.load(open(sys.argv[1]))
b = data["bootstrapper"]
print("bootstrapper", b["core_node"], b["ip"])
for node_id, info in data["nodes"].items():
    print(node_id, info["core_node"], info["ip"], *info.get("args", []))
EOF
)
BOOT_NODE=$(echo "$TABLE" | awk '$1=="bootstrapper"{print $2}')
BOOT_IP=$(echo "$TABLE" | awk '$1=="bootstrapper"{print $3}')
ALL_IDS=$(echo "$TABLE" | awk '$1!="bootstrapper"{print $1}')

field() { echo "$TABLE" | awk -v id="$1" -v n="$2" '$1==id{print $n}'; }
args_of() { echo "$TABLE" | awk -v id="$1" '$1==id{for (i = 4; i <= NF; i++) printf "%s ", $i}'; }
core_node_of() {
    local cn; cn=$(field "$1" 2)
    [ -n "$cn" ] || { echo "ID desconhecido: $1" >&2; exit 1; }
    echo "$cn"
}
in_node() { local cn=$1; shift; vcmd -c "$SESSION/$cn" -- "$@"; }
pid_of() { cat "$RUN_DIR/$1.pid" 2>/dev/null || true; }
running() {
    local pid; pid=$(pid_of "$1")
    [ -n "$pid" ] && in_node "$(core_node_of "$1")" kill -0 "$pid" 2>/dev/null
}

# Lança um processo em background dentro de um nó do CORE, com stdin ligado a um FIFO
launch() {
    local name=$1 cn=$2; shift 2
    if running "$name"; then echo "$name já está a correr (PID $(pid_of "$name"))"; return; fi
    [ -S "$SESSION/$cn" ] || { echo "o nó $cn não existe na sessão $SESSION"; exit 1; }
    rm -f "$RUN_DIR/$name.cmd"; mkfifo "$RUN_DIR/$name.cmd"
    rm -f "$RUN_DIR/$name.pid"
    # "<>" abre o FIFO para leitura e escrita: não bloqueia e o processo nunca recebe EOF.
    # O sh interior escreve o seu PID e faz exec do python, por isso o PID guardado é o do python.
    in_node "$cn" sh -c "cd '$PROJECT' && { sh -c 'echo \$\$ > $RUN_DIR/$name.pid; exec python3 -u $*' <>'$RUN_DIR/$name.cmd' >>'logs/$name.out' 2>&1 & }"
    sleep 0.5
    if running "$name"; then
        echo "$name arrancado no $cn (PID $(pid_of "$name"))"
    else
        echo "ERRO: $name terminou logo após arrancar (PID guardado: '$(pid_of "$name")'):"
        tail -n 5 "logs/$name.out" 2>/dev/null || echo "  (logs/$name.out não existe)"
    fi
}

# Espera que o OSPF tenha convergido: o nó do bootstrapper tem de chegar a todos os nós overlay
wait_routes() {
    local id ip ok
    echo -n "à espera das rotas do OSPF"
    for _ in $(seq 60); do
        ok=1
        for id in $ALL_IDS; do
            ip=$(field "$id" 3)
            in_node "$BOOT_NODE" ping -c1 -W1 "$ip" >/dev/null 2>&1 || { ok=0; break; }
        done
        [ "$ok" = 1 ] && { echo " ok"; return; }
        echo -n "."; sleep 1
    done
    echo; echo "AVISO: o $BOOT_NODE ainda não chega ao $id ($ip); a arrancar na mesma"
}

send_cmd() {
    local id=$1 text=$2 size
    running "$id" || { echo "$id não está a correr"; return 1; }
    size=$(stat -c %s "logs/$id.out" 2>/dev/null || echo 0)
    timeout 2 sh -c "echo '$text' > '$RUN_DIR/$id.cmd'"
    sleep 0.5
    tail -c +$((size + 1)) "logs/$id.out"
}

stop_ids() {
    local id pid
    for id in "$@"; do running "$id" && timeout 2 sh -c "echo sair > '$RUN_DIR/$id.cmd'" || true; done
    for id in "$@"; do
        pid=$(pid_of "$id"); [ -n "$pid" ] || continue
        for _ in $(seq 30); do running "$id" || break; sleep 0.1; done
        if running "$id"; then in_node "$(core_node_of "$id")" kill -9 "$pid"; echo "$id: morto à força"
        else echo "$id: terminado"; fi
        rm -f "$RUN_DIR/$id.pid" "$RUN_DIR/$id.cmd"
    done
}

signal_node() {
    local sig=$1 id=$2
    running "$id" || { echo "$id não está a correr"; exit 1; }
    in_node "$(core_node_of "$id")" kill "-$sig" "$(pid_of "$id")"
    echo "$id: enviado SIG$sig"
}

CMD=${1:-}; shift || true
case "$CMD" in
    start)
        IDS=${*:-$ALL_IDS}
        if ! running bootstrapper; then
            if [ $# -eq 0 ]; then rm -f logs/*.log logs/*.out; fi   # arranque completo: logs limpos
            wait_routes
            launch bootstrapper "$BOOT_NODE" -m ott.bootstrapper "$CONFIG"
        fi
        for id in $IDS; do
            launch "$id" "$(core_node_of "$id")" -m ott.node "$id" "$BOOT_IP" $(args_of "$id")
        done
        echo "estado: sudo scripts/core.sh cmd <ID> vizinhos   |   logs: tail -f logs/<ID>.log"
        ;;
    stop)
        if [ $# -gt 0 ]; then stop_ids "$@"; else
            stop_ids $ALL_IDS
            if running bootstrapper; then
                in_node "$BOOT_NODE" kill "$(pid_of bootstrapper)"; echo "bootstrapper: terminado"
            fi
            rm -f "$RUN_DIR/bootstrapper.pid" "$RUN_DIR/bootstrapper.cmd"
            # Processos perdidos (sem PID guardado, ex.: de um arranque falhado)
            if pkill -9 -f "python3 -u -m ott\." 2>/dev/null; then echo "processos perdidos do ott terminados"; fi
        fi
        ;;
    status)
        echo "sessão: $SESSION"
        for id in bootstrapper $ALL_IDS; do
            if running "$id"; then echo "  $id ($(core_node_of "$id")): a correr, PID $(pid_of "$id")"
            else echo "  $id ($(core_node_of "$id")): parado"; fi
        done
        ;;
    cmd)      [ $# -ge 2 ] || { echo "uso: scripts/core.sh cmd <ID> <comando>"; exit 1; }; send_cmd "$1" "$2" ;;
    crash)    signal_node 9 "${1:?ID em falta}" ;;
    freeze)   signal_node STOP "${1:?ID em falta}" ;;
    unfreeze) signal_node CONT "${1:?ID em falta}" ;;
    *)        sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
