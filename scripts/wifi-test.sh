#!/bin/bash
# Передача файла через настоящие службы. wifi-test.sh <to-deck|to-mac> <путь на источнике> <папка назначения>
DECK=${DECK:?укажите DECK=deck@адрес}
M="$(cd "$(dirname "$0")/.." && pwd)/target/release/deckdrop"
D='$HOME/.local/bin/deckdrop'
dir=$1; src=$2; dest=$3
if [ "$dir" = to-deck ]; then
  job=$("$M" send "Steam Deck" "$src" --to "$dest" --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["job"])')
  stat() { "$M" status --json; }
  recv() { ssh "$DECK" "$D $*"; }
else
  name=$("$M" status --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["me"]["name"])')
  job=$(ssh "$DECK" "$D send '$name' '$src' --to '$dest' --json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["job"])')
  stat() { ssh "$DECK" "$D status --json"; }
  recv() { "$M" "$@"; }
fi
echo "задание $job"
for i in $(seq 1 40); do
  if [ "$dir" = to-deck ]; then O=$(ssh "$DECK" "$D status --json"); else O=$("$M" status --json); fi
  O=$(echo "$O" | python3 -c 'import json,sys;o=json.load(sys.stdin)["offers"];print(o[0]["offer"] if o else "")')
  [ -n "$O" ] && break; sleep 0.5
done
if [ -n "$O" ]; then recv accept "$O" --to "$dest"; else echo "предложения нет (приём без вопроса?)"; fi
T0=$(date +%s)
while :; do
  S=$(stat | python3 -c '
import json,sys
j=[x for x in json.load(sys.stdin)["jobs"] if x["id"]=="'$job'"][0]
print(j["state"],j["phase"],"%.2f ГБ"%(j["bytes"]/1e9),"сейчас %.1f"%(j["speed"]/1e6),"среднее %.1f"%(j["avg_speed"]/1e6),"предел %.0f"%(j["rate"]/1e6),"rtt %.1f мс"%(j["rtt_us"]/1e3),"повторы",j["retx"],"из",j["sent_pkts"],"заново",j["repaired"],j["stalls"],j["message"])')
  echo "$(( $(date +%s)-T0 ))с $S"
  case "$S" in done*|failed*|cancelled*|declined*) break;; esac
  sleep 2
done
