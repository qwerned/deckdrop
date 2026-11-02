#!/bin/bash
# Обратное направление: Deck отправляет папку на Мак, служба Мака падает на ходу.
# big-test-back.sh <папка на Deck> <папка назначения на Маке>
cd "$(dirname "$0")/.."
SRC="$1"; DST="$2"; M=target/release/deckdrop; DECK=${DECK:?укажите DECK=deck@адрес}; D='$HOME/.local/bin/deckdrop'
rm -rf "$DST"; mkdir -p "$DST"
JOB=$(ssh $DECK "$D send 'MacBook Air' $SRC --to '$DST' --json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["job"])'); echo "задание $JOB"
for i in $(seq 1 40); do O=$($M status --json | python3 -c 'import json,sys;o=json.load(sys.stdin)["offers"];print(o[0]["offer"] if o else "")'); [ -n "$O" ] && break; sleep 0.5; done
$M accept "$O" --to "$DST"
T0=$(date +%s); E1=; E2=
while :; do
  L=$(ssh $DECK "$D status --json" | python3 -c '
import json,sys
j=[x for x in json.load(sys.stdin)["jobs"] if x["id"]=="'$JOB'"][0]
print(j["state"],j["phase"],int(j["bytes"]/1e6),"%.1f"%(j["speed"]/1e6),"%.1f"%(j["avg_speed"]/1e6),"предел %.0f"%(j["rate"]/1e6),"повторы",j["retx"],"из",j["sent_pkts"],"заново",j["repaired"],j["root"][:16],j["message"])')
  echo "$(( $(date +%s)-T0 ))с $L"
  set -- $L; MB=$3
  if [ -z "$E1" ] && [ "${MB:-0}" -gt 800 ]; then E1=1; echo ">>> пауза 10 с"; ssh $DECK "$D pause $JOB"; sleep 10; ssh $DECK "$D resume $JOB"; echo ">>> продолжаю"; fi
  if [ -z "$E2" ] && [ "${MB:-0}" -gt 2500 ]; then E2=1; echo ">>> kill -9 службы на Маке, запуск через 5 с"; pkill -9 -f '[d]eckdrop daemon'; sleep 5; nohup $M daemon </dev/null >/dev/null 2>&1 & fi
  case "$L" in done*|failed*|cancelled*|declined*) break;; esac
  sleep 5
done
echo "== независимая сверка"
N=$(basename "$SRC")
ssh $DECK "cd \$(dirname $SRC) && find $N -type f -print0 | sort -z | xargs -0 sha256sum" | awk '{print $1"  "$2}' | shasum -a 256 | cut -d' ' -f1
(cd "$DST" && find "$N" -type f -print0 | sort -z | xargs -0 shasum -a 256 | awk '{print $1"  "$2}' | shasum -a 256 | cut -d' ' -f1)
ls -A "$DST"; stat -f '%Sm %N' -t '%F %T' "$DST/$N/sub/f1.dat"
echo ГОТОВО
