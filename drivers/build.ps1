# NovusDB Drivers — Build Script (Windows)
# Requires: Go, GCC (MinGW-w64)
#
# Usage:
#   .\drivers\build.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

Write-Host "=== NovusDB C Shared Library Build ===" -ForegroundColor Cyan
Write-Host ""

# Check prerequisites
$gcc = Get-Command gcc -ErrorAction SilentlyContinue
if (-not $gcc) {
    Write-Host "ERROR: gcc not found. Install MinGW-w64:" -ForegroundColor Red
    Write-Host "  choco install mingw   (admin required)" -ForegroundColor Yellow
    Write-Host "  or download from https://www.mingw-w64.org/" -ForegroundColor Yellow
    exit 1
}

$go = Get-Command go -ErrorAction SilentlyContinue
if (-not $go) {
    Write-Host "ERROR: go not found." -ForegroundColor Red
    exit 1
}

Write-Host "Go:  $(go version)"
Write-Host "GCC: $(gcc --version | Select-Object -First 1)"
Write-Host ""

# Build shared library
$env:CGO_ENABLED = "1"
$outDll = Join-Path $root "drivers\c\novusdb.dll"

Write-Host "Building novusdb.dll ..." -ForegroundColor Yellow
Push-Location $root
go build -buildmode=c-shared -o $outDll ./drivers/c/
Pop-Location

if (Test-Path $outDll) {
    $size = [math]::Round((Get-Item $outDll).Length / 1MB, 2)
    Write-Host ""
    Write-Host "SUCCESS: $outDll ($size MB)" -ForegroundColor Green
    Write-Host ""
    Write-Host "Files produced:" -ForegroundColor Cyan
    Get-ChildItem (Join-Path $root "drivers\c\NovusDB.*") | ForEach-Object {
        Write-Host "  $($_.Name) ($([math]::Round($_.Length / 1KB, 1)) KB)"
    }

    # Copy DLL to driver directories for convenience
    Copy-Item $outDll (Join-Path $root "drivers\python\novusdb.dll") -Force
    Copy-Item $outDll (Join-Path $root "drivers\node\novusdb.dll") -Force
    Copy-Item $outDll (Join-Path $root "drivers\java\novusdb.dll") -Force
    Write-Host ""
    Write-Host "DLL copied to python/, node/, java/ directories." -ForegroundColor Green
} else {
    Write-Host "ERROR: Build failed." -ForegroundColor Red
    exit 1
}
