#!/bin/bash
# Очередь: три задания подряд к одному устройству, среднее убрано из очереди.
# Использование: e2e-queue.sh <рабочая папка>
set -u
S="$1"
B="$(cd "$(dirname "$0")/.." && pwd)/target/release/deckdrop"
pkill -f "^$B daemon" 2>/dev/null; sleep 0.5
rm -rf "$S"; mkdir -p "$S"/{a,b,dst}
for n in one two three; do mkdir -p "$S/src/$n"; dd if=/dev/urandom of="$S/src/$n/data.bin" bs=1m count=200 2>/dev/null; done
a() { DECKDROP_HOME="$S/a" DECKDROP_PORT=52791 DECKDROP_NAME=Mac DECKDROP_PEERS=127.0.0.1:52792 "$B" "$@"; }
z() { DECKDROP_HOME="$S/b" DECKDROP_PORT=52792 DECKDROP_NAME=Deck DECKDROP_PEERS=127.0.0.1:52791 "$B" "$@"; }
a daemon 2>&1 | sed 's/^/[a] /' &
z daemon 2>&1 | sed 's/^/[z] /' &
trap 'pkill -f "^$B daemon" 2>/dev/null' EXIT
sleep 3
a pair Deck > "$S/pair.txt" 2>&1 & PP=$!
sleep 1
z pair-enter "$(grep -oE '[0-9]{6}' "$S/pair.txt")"; wait $PP
sleep 3
J1=$(a send Deck "$S/src/one" --to "$S/dst" --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["job"])')
J2=$(a send Deck "$S/src/two" --to "$S/dst" --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["job"])')
J3=$(a send Deck "$S/src/three" --to "$S/dst" --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["job"])')
show() { a status --json | python3 -c 'import json,sys;print([(j["name"],j["state"],round(j["total"]/1e6)) for j in reversed(json.load(sys.stdin)["jobs"])])'; }
sleep 1; echo "после постановки: $(show)"
a cancel "$J2"; sleep 0.5; echo "убрал two: $(show)"
T0=$(date +%s)
while :; do
  O=$(z status --json | python3 -c 'import json,sys;o=json.load(sys.stdin)["offers"];print(o[0]["offer"] if o else "")')
  [ -n "$O" ] && { echo "принимаю $O: $(show)"; z accept "$O" --to "$S/dst"; }
  ST=$(show); echo "$ST"
  case "$ST" in *running*|*connecting*|*queued*) ;; *) break;; esac
  [ $(( $(date +%s) - T0 )) -gt 60 ] && { echo ТАЙМАУТ; break; }
  sleep 0.5
done
echo "== сверка"; ls "$S/dst"
for n in one three; do cmp "$S/src/$n/data.bin" "$S/dst/$n/data.bin" && echo "$n совпал"; done
[ -e "$S/dst/two" ] && echo "ОШИБКА: two дошёл" || echo "two не передавался"
