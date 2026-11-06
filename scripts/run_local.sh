#!/usr/bin/env bash
# Arranca o bootstrapper (127.0.0.1) e todos os nós de um ficheiro de topologia, em background.
# Uso: scripts/run_local.sh <ficheiro_topologia> [--reverse] [--boot-last]
#   --reverse    arranca os nós pela ordem inversa à do ficheiro
#   --boot-last  arranca os nós primeiro e o bootstrapper no fim (testa as novas tentativas de registo)
# Saída em logs/ (ver scripts/start_node.sh). Terminar tudo: scripts/stop_local.sh
# O bootstrapper usa 127.0.0.1, um IP que não é de nenhum nó overlay, tal como no CORE tem um host dedicado.
# Os parâmetros do overlay (probes, custo, gossip, reparação) estão em ott/config.py.
set -euo pipefail
cd "$(dirname "$0")/.."

TOPOLOGY="$1"; shift
REVERSE=0; BOOT_LAST=0
for arg in "$@"; do
    case "$arg" in
        --reverse) REVERSE=1 ;;
        --boot-last) BOOT_LAST=1 ;;
        *) echo "opção desconhecida: $arg"; exit 1 ;;
    esac
done

if ls logs/*.pid >/dev/null 2>&1; then
    echo "já há processos de uma execução anterior: corre scripts/stop_local.sh primeiro"; exit 1
fi
rm -rf logs; mkdir -p logs

start_bootstrapper() {
    python3 -u -m ott.bootstrapper "$TOPOLOGY" --bind 127.0.0.1 </dev/null >>logs/bootstrapper.out 2>&1 &
    echo $! > logs/bootstrapper.pid
    sleep 0.3
    if ! kill -0 "$(cat logs/bootstrapper.pid)" 2>/dev/null; then
        # Ex.: porta 4999 ocupada por um bootstrapper antigo -> os nós falariam com o errado
        echo "ERRO: o bootstrapper não arrancou:"; cat logs/bootstrapper.out; exit 1
    fi
    echo "bootstrapper arrancado em 127.0.0.1 (PID $(cat logs/bootstrapper.pid))"
}

NODES=$(python3 -c 'import json,sys; print(" ".join(json.load(open(sys.argv[1]))["nodes"]))' "$TOPOLOGY")
if [ "$REVERSE" = 1 ]; then NODES=$(echo "$NODES" | tr ' ' '\n' | tac | tr '\n' ' '); fi

[ "$BOOT_LAST" = 0 ] && start_bootstrapper
for node in $NODES; do
    scripts/start_node.sh "$TOPOLOGY" "$node"
done
[ "$BOOT_LAST" = 1 ] && start_bootstrapper

# Um nó termina logo se as portas estiverem ocupadas (ex.: 5002/UDP de uma execução antiga)
sleep 0.5
for node in $NODES; do
    if ! kill -0 "$(cat "logs/$node.pid")" 2>/dev/null; then
        echo "ERRO: o nó $node terminou logo após arrancar:"; tail -n 5 "logs/$node.out"; exit 1
    fi
done
echo "comandos: echo vizinhos|metricas|candidatos|sair > logs/<ID>.cmd (resposta em logs/<ID>.out)"
exit 0
