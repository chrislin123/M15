[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 1. Cleanup
if (Test-Path "./dist") {
    Write-Host "[1/2] Old 'dist' folder detected. Cleaning up..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force "./dist"
}

# 2. PyArmor
Write-Host "[2/2] Running PyArmor obfuscation..." -ForegroundColor Cyan
pyarmor gen --recursive --exclude ./.venv --exclude ./dist .

# 3. Done
Write-Host "---------------------------------------"
Write-Host "Build Completed Successfully!" -ForegroundColor Green
ii ./dist