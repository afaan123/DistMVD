#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# Default config lives next to this script; cwd becomes ROOT_DIR for dataset paths in the conf.
CONFIG_FILE="${1:-$SCRIPT_DIR/experiments.conf}"
# Resolve the config path to absolute now, before we cd into ROOT_DIR,
# so a relative path passed on the command line keeps working.
CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"

cd "$ROOT_DIR"

mkdir -p build

javac -d build \
  metanome/LocalTiming.java \
  metanome/MvdFinder.java \
  metanome/ColumnBasedMvdFinder.java \
  metanome/MvDAlgorithmConfig.java \
  metanome/MvD.java \
  metanome/Relation.java \
  metanome/Tuple.java \
  metanome/StringTuple.java \
  metanome/PositionListIndex.java

java -cp build de.metanome.algorithms.mvddet.LocalTiming "$CONFIG_FILE"