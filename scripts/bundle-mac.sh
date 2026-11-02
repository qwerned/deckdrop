#!/bin/bash
# Собирает DeckDrop.app и кладёт в /Applications.
set -e
cd "$(dirname "$0")/.."
A=target/DeckDrop.app
rm -rf "$A"; mkdir -p "$A/Contents/MacOS"
cp target/release/deckdrop "$A/Contents/MacOS/deckdrop"
cp crates/deckdrop/Info.plist "$A/Contents/Info.plist"
codesign --force --sign - "$A" 2>&1 | tail -1
rm -rf /Applications/DeckDrop.app; cp -R "$A" /Applications/DeckDrop.app
echo установлено: /Applications/DeckDrop.app
