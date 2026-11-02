#!/bin/bash
# Собирает архив плагина Decky: DeckDrop.zip для установки из режима разработчика.
set -e
cd "$(dirname "$0")/../decky"
pnpm run build >/dev/null 2>&1 || true
rm -rf ../target/decky; mkdir -p ../target/decky/DeckDrop/dist
cp plugin.json package.json main.py ../target/decky/DeckDrop/
cp dist/index.js ../target/decky/DeckDrop/dist/
(cd ../target/decky && zip -qr DeckDrop.zip DeckDrop)
ls -la ../target/decky/DeckDrop.zip
