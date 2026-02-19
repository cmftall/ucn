#!/usr/bin/env bash
set -euo pipefail

TIR_OUTPUT_PATH="${1:-/tmp/dsfi_tir_output}"
FLAG_PATH="${TIR_OUTPUT_PATH}/PERIMETRAGE_DACD_VIDE"

if [ -f "${FLAG_PATH}" ]; then
    echo "Flag ${FLAG_PATH} detecte: perimetrage DACD vide, arret propre du traitement."
    exit 0
fi

echo "Aucun flag detecte, poursuite des controles mono-periodes."
exit 0

