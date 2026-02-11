#!/bin/bash
# NovusDB Drivers — Build Script (Linux/macOS)
# Requires: Go, GCC
#
# Usage:
#   chmod +x drivers/build.sh
#   ./drivers/build.sh

set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== NovusDB C Shared Library Build ==="
echo ""

# Check prerequisites
if ! command -v gcc &> /dev/null; then
    echo "ERROR: gcc not found. Install it:"
    echo "  Ubuntu/Debian: sudo apt install gcc"
    echo "  macOS: xcode-select --install"
    exit 1
fi

if ! command -v go &> /dev/null; then
    echo "ERROR: go not found."
    exit 1
fi

echo "Go:  $(go version)"
echo "GCC: $(gcc --version | head -1)"
echo ""

# Detect OS
OS=$(uname -s)
if [ "$OS" = "Darwin" ]; then
    OUT="$ROOT/drivers/c/libnovusdb.dylib"
    EXT="dylib"
else
    OUT="$ROOT/drivers/c/libnovusdb.so"
    EXT="so"
fi

export CGO_ENABLED=1

echo "Building libNovusDB.$EXT ..."
cd "$ROOT"
go build -buildmode=c-shared -o "$OUT" ./drivers/c/

if [ -f "$OUT" ]; then
    SIZE=$(du -h "$OUT" | cut -f1)
    echo ""
    echo "SUCCESS: $OUT ($SIZE)"
    echo ""
    echo "Files produced:"
    ls -lh "$ROOT/drivers/c/libNovusDB."*
    
    # Copy to driver directories
    cp "$OUT" "$ROOT/drivers/python/"
    cp "$OUT" "$ROOT/drivers/node/"
    cp "$OUT" "$ROOT/drivers/java/"
    echo ""
    echo "Library copied to python/, node/, java/ directories."
else
    echo "ERROR: Build failed."
    exit 1
fi
