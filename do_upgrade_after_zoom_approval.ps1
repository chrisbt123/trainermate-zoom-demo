param(
    [switch]$ApprovalConfirmed,
    [switch]$ApplyDeploymentSwitch,
    [switch]$CommitAndPush
)

$ErrorActionPreference = "Stop"

function Step($Message) {
    Write-Host ""
    Write-Host "== $Message ==" -ForegroundColor Cyan
}

function Fail($Message) {
    Write-Host ""
    Write-Host "Upgrade stopped: $Message" -ForegroundColor Red
    exit 1
}

if (-not $ApprovalConfirmed) {
    Fail "Zoom approval has not been explicitly confirmed. Re-run with -ApprovalConfirmed after Marketplace approval is visible."
}

$Repo = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Repo

Step "Checking repository state"
$status = git status --porcelain
if ($status) {
    Write-Host $status
    Fail "working tree is not clean. Commit, stash, or revert unrelated changes first."
}

$Python = "python"
$BundledPython = "C:\Users\chris\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if (Test-Path $BundledPython) {
    $Python = $BundledPython
}

Step "Verifying Zoom review surface before tagging"
& $Python verify_zoom_review_freeze.py
if ($LASTEXITCODE -ne 0) { Fail "Zoom review freeze verification failed." }

$today = Get-Date -Format "yyyy-MM-dd"
$tag = "zoom-approved-$today"
$existingTag = git tag --list $tag
if (-not $existingTag) {
    Step "Tagging exact approved revision as $tag"
    git tag -a $tag -m "Zoom approved TrainerMate revision $today"
} else {
    Write-Host "Tag $tag already exists; leaving it in place."
}

Step "Running baseline checks"
& $Python -m py_compile app.py main.py dashboard_app.py verify_trainermate.py verify_zoom_review_freeze.py
if ($LASTEXITCODE -ne 0) { Fail "Python compile check failed." }
& $Python verify_trainermate.py
if ($LASTEXITCODE -ne 0) { Fail "TrainerMate verification failed." }

if (-not $ApplyDeploymentSwitch) {
    Write-Host ""
    Write-Host "Approval tag/checks are complete. Deployment files were not changed because -ApplyDeploymentSwitch was not supplied." -ForegroundColor Yellow
    Write-Host "To switch Render to main.py, re-run with: .\do_upgrade_after_zoom_approval.ps1 -ApprovalConfirmed -ApplyDeploymentSwitch -CommitAndPush"
    exit 0
}

Step "Backing up deployment files"
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backupDir = Join-Path $Repo ".upgrade-backups\$stamp"
New-Item -ItemType Directory -Force -Path $backupDir | Out-Null
Copy-Item -LiteralPath "Procfile" -Destination (Join-Path $backupDir "Procfile")
Copy-Item -LiteralPath "render.yaml" -Destination (Join-Path $backupDir "render.yaml")

Step "Switching Render entrypoint to main.py"
$procfile = Get-Content -Raw -LiteralPath "Procfile"
$procfile = $procfile -replace "web:\s*gunicorn app:app", 'web: uvicorn main:app --host 0.0.0.0 --port $PORT'
Set-Content -LiteralPath "Procfile" -Value $procfile -Encoding UTF8

$render = Get-Content -Raw -LiteralPath "render.yaml"
$render = $render -replace "startCommand:\s*gunicorn app:app", 'startCommand: uvicorn main:app --host 0.0.0.0 --port $PORT'
$render = $render -replace 'key:\s*TRAINERMATE_REVIEWER_DEMO\s*\r?\n\s*value:\s*"1"', "key: TRAINERMATE_REVIEWER_DEMO`r`n        value: `"0`""
$render = $render -replace 'key:\s*TRAINERMATE_REMOTE_ADMIN\s*\r?\n\s*value:\s*"0"', "key: TRAINERMATE_REMOTE_ADMIN`r`n        value: `"1`""
Set-Content -LiteralPath "render.yaml" -Value $render -Encoding UTF8

Step "Running checks after deployment switch"
& $Python -m py_compile app.py main.py dashboard_app.py verify_trainermate.py verify_zoom_review_freeze.py
if ($LASTEXITCODE -ne 0) { Fail "Python compile check failed after deployment switch." }
& $Python verify_trainermate.py
if ($LASTEXITCODE -ne 0) { Fail "TrainerMate verification failed after deployment switch." }

Step "Changed files"
git status --short
git diff -- Procfile render.yaml

Write-Host ""
Write-Host "Upgrade file switch is ready." -ForegroundColor Green
Write-Host "Backup folder: $backupDir"

if ($CommitAndPush) {
    Step "Committing and pushing upgrade"
    git add Procfile render.yaml POST_APPROVAL_UPGRADE_TODO.md do_upgrade_after_zoom_approval.ps1
    git commit -m "Switch Render to TrainerMate control API after Zoom approval"
    git push origin main
    git push origin $tag
    Write-Host ""
    Write-Host "Upgrade pushed. Render should now redeploy from main." -ForegroundColor Green
} else {
    Write-Host "Next: review the diff, then commit and push. Render will redeploy from the pushed deployment files."
    Write-Host "Or re-run with -CommitAndPush after reviewing."
}
