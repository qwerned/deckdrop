#!/bin/bash
# Матрица замеров Мак ↔ Deck. Вывод идёт в лог по ходу работы.
DECK=${DECK:?укажите DECK=deck@адрес}
DECK_IP=${DECK#*@}
MAC_IP=$(ipconfig getifaddr en0)
L=$(cd "$(dirname "$0")/.." && pwd)/target/release/ddbench
R=./ddbench
SECS=${SECS:-8}
PORT=52800
final() { grep -E 'ИТОГ' ; }
m2d() { # название, опции приёма, опции отправки
  echo; echo "### Мак → Deck: $1"; PORT=$((PORT+4))
  ssh "$DECK" "timeout $((SECS+15)) $R $2 --port $PORT" 2>&1 | final & sleep 1.5
  "$L" $3 --to "$DECK_IP" --port $PORT --secs $SECS 2>&1 | final; wait
}
d2m() {
  echo; echo "### Deck → Мак: $1"; PORT=$((PORT+4))
  timeout_recv=$((SECS+15)); "$L" $2 --port $PORT 2>&1 | final & P=$!; sleep 1
  ssh "$DECK" "$R $3 --to $MAC_IP --port $PORT --secs $SECS" 2>&1 | final
  ( sleep $timeout_recv; kill $P 2>/dev/null ) & K=$!; wait $P; kill $K 2>/dev/null
}
echo "сигнал Deck: $(ssh "$DECK" "iw dev wlan0 link | grep -E 'signal|tx bitrate|rx bitrate'" | tr '\n' ' ')"
for round in 1 2; do
echo; echo "===== круг $round ====="
m2d "UDP без предела, пачки 64" "udp-recv --batch 64" "udp-send --batch 64"
m2d "UDP 120 МБ/с, пачки 64" "udp-recv --batch 64" "udp-send --batch 64 --rate 120"
m2d "UDP 100 МБ/с, по одному" "udp-recv --batch 1" "udp-send --batch 1 --rate 100"
m2d "UDP без предела, приём GRO" "udp-recv --batch 16 --gro" "udp-send --batch 64"
m2d "UDP без предела, 2 потока" "udp-recv --batch 64 --threads 2" "udp-send --batch 64 --threads 2"
m2d "TCP 1 поток" "tcp-recv --streams 1" "tcp-send --streams 1"
m2d "TCP 4 потока" "tcp-recv --streams 4" "tcp-send --streams 4"
m2d "QUIC cubic" "quic-recv --cc cubic" "quic-send --cc cubic"
m2d "QUIC bbr" "quic-recv --cc bbr" "quic-send --cc bbr"
d2m "UDP без предела, пачки 64" "udp-recv --batch 64" "udp-send --batch 64"
d2m "UDP без предела, GSO" "udp-recv --batch 64" "udp-send --batch 44 --gso"
d2m "UDP 100 МБ/с, по одному" "udp-recv --batch 1" "udp-send --batch 1 --rate 100"
d2m "UDP без предела, 2 потока" "udp-recv --batch 64 --threads 2" "udp-send --batch 64 --threads 2"
d2m "TCP 1 поток" "tcp-recv --streams 1" "tcp-send --streams 1"
d2m "TCP 4 потока" "tcp-recv --streams 4" "tcp-send --streams 4"
d2m "QUIC cubic" "quic-recv --cc cubic" "quic-send --cc cubic"
d2m "QUIC bbr" "quic-recv --cc bbr" "quic-send --cc bbr"
done
echo; echo "ГОТОВО"
