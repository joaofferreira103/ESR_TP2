#!/usr/bin/env bash
# Arranca UM nó do ficheiro de topologia, em background, para testes locais.
# Uso: scripts/start_node.sh <ficheiro_topologia> <ID> [opções extra do ott.node, ex.: --server]
#
# - IP (--bind) lido do ficheiro de topologia; bootstrapper em 127.0.0.1.
# - logs/<ID>.log: log do nó (escrito pelo próprio nó)
#   logs/<ID>.out: stdout/stderr do processo (inclui tracebacks, se houver)
#   logs/<ID>.pid: PID do processo
#   logs/<ID>.cmd: FIFO ligado ao stdin do nó -> echo <comando> > logs/<ID>.cmd
#                  comandos: vizinhos | metricas | candidatos | sair (a resposta aparece em logs/<ID>.out)
set -euo pipefail
cd "$(dirname "$0")/.."

TOPOLOGY="$1"; NODE_ID="$2"; shift 2
IP=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["nodes"][sys.argv[2]]["ip"])' "$TOPOLOGY" "$NODE_ID")

mkdir -p logs
if [ -f "logs/$NODE_ID.pid" ] && kill -0 "$(cat "logs/$NODE_ID.pid")" 2>/dev/null; then
    echo "$NODE_ID já está a correr (PID $(cat "logs/$NODE_ID.pid"))"; exit 1
fi
rm -f "logs/$NODE_ID.cmd"; mkfifo "logs/$NODE_ID.cmd"

# "<>" abre o FIFO para leitura e escrita: não bloqueia à espera de um escritor
# e o nó nunca recebe EOF quando um "echo" termina
python3 -u -m ott.node "$NODE_ID" 127.0.0.1 --bind "$IP" "$@" \
    <>"logs/$NODE_ID.cmd" >>"logs/$NODE_ID.out" 2>&1 &
echo $! > "logs/$NODE_ID.pid"
echo "$NODE_ID arrancado em $IP (PID $!)"
