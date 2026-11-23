param(
    [string]$Message = "更新桥接服务 bridge.py",
    [string]$Tag = "",
    [switch]$DryRun,
    [switch]$NoSync
)

# 桥接服务一键更新脚本（UTF-8 BOM，Windows PowerShell 5.1 可直接运行）
# 普通用法：.\update-from-github.ps1
# 指定说明：.\update-from-github.ps1 -Message "修复验收状态回写"
# 带版本号：.\update-from-github.ps1 -Tag v1.1.0
# 试运行：  .\update-from-github.ps1 -DryRun
# 不同步上层 bridge.py：加 -NoSync

$ErrorActionPreference = "Stop"

function Write-Step($text) { Write-Host "== $text ==" -ForegroundColor Cyan }
function Fail($text) { Write-Host $text -ForegroundColor Red; exit 1 }

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$liveBridge = Join-Path (Split-Path -Parent $repo) "bridge.py"
Set-Location $repo

if (-not (Test-Path -LiteralPath ".git")) { Fail "当前目录不是 Git 仓库：$repo" }

if ($Tag -and $Tag -notmatch '^v\d+\.\d+\.\d+$') {
    Fail "Tag 必须形如 v1.1.0（vMAJOR.MINOR.PATCH），收到：$Tag"
}

$branch = git rev-parse --abbrev-ref HEAD
if ($LASTEXITCODE -ne 0) { Fail "无法读取当前分支" }
if ($branch -ne "main") { Fail "必须在 main 分支，当前是：$branch" }

$remoteUrl = git remote get-url origin
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($remoteUrl)) { Fail "未配置 origin 远程" }
$web = ($remoteUrl -replace '\.git$', '') -replace '^git@github\.com:', 'https://github.com/'

Write-Step "第 1 步：同步桥接源码"
if ($DryRun) {
    if ($NoSync) { Write-Host "[DryRun] 已指定 -NoSync，跳过同步" }
    else { Write-Host "[DryRun] 将上层 bridge.py 同步到本仓库（加 -NoSync 可跳过）" }
} elseif ($NoSync) {
    Write-Host "已指定 -NoSync，跳过同步"
} elseif (Test-Path -LiteralPath $liveBridge) {
    $liveHash = (Get-FileHash -LiteralPath $liveBridge -Algorithm SHA256).Hash
    $repoHash = (Get-FileHash -LiteralPath (Join-Path $repo "bridge.py") -Algorithm SHA256).Hash
    if ($liveHash -eq $repoHash) {
        Write-Host "bridge.py 与上层一致，无需复制"
    } else {
        Copy-Item -LiteralPath $liveBridge -Destination (Join-Path $repo "bridge.py") -Force
        Write-Host "已从 $liveBridge 复制最新 bridge.py" -ForegroundColor Green
    }
} else {
    Write-Host "未找到 $liveBridge，跳过同步"
}

Write-Step "第 2 步：检查远端"
if ($DryRun) {
    Write-Host "[DryRun] git fetch origin main"
    Write-Host "[DryRun] 统计本地领先/落后提交数"
} else {
    git fetch origin main
    if ($LASTEXITCODE -ne 0) { Fail "fetch 失败：检查网络或 GitHub 登录状态" }
}

$behind = [int](git rev-list --count HEAD..origin/main)
$ahead  = [int](git rev-list --count origin/main..HEAD)
Write-Host "本地领先 $ahead 个提交，落后 $behind 个提交（DryRun 时基于上次 fetch）"

Write-Step "第 3 步：提交本地改动"
if ($DryRun) {
    Write-Host "[DryRun] git add -A"
    Write-Host "[DryRun] git commit -m `"$Message`" （仅有改动时）"
} else {
    git add -A
    if ($LASTEXITCODE -ne 0) { Fail "git add -A 失败" }
    if (git diff --cached --quiet) {
        Write-Host "没有待提交改动，跳过 commit"
    } else {
        git commit -m $Message
        if ($LASTEXITCODE -ne 0) { Fail "commit 失败" }
        Write-Host "已提交：$Message" -ForegroundColor Green
    }
    $ahead = [int](git rev-list --count origin/main..HEAD)
}

Write-Step "第 4 步：处理远端新提交并校验祖先关系"
if ($DryRun) {
    Write-Host "[DryRun] 若有落后提交则 git pull --rebase origin main"
    Write-Host "[DryRun] git merge-base --is-ancestor origin/main HEAD"
} else {
    if ($behind -gt 0 -and $ahead -gt 0) {
        git pull --rebase origin main
        if ($LASTEXITCODE -ne 0) { Fail "rebase 冲突：手动 git rebase --continue 或 git rebase --abort 后重跑" }
    } elseif ($behind -gt 0) {
        git pull --ff-only origin main
        if ($LASTEXITCODE -ne 0) { Fail "快进拉取失败：请手动处理本地改动" }
    } else {
        Write-Host "本地已是最新，无需合并"
    }
    git merge-base --is-ancestor origin/main HEAD
    if ($LASTEXITCODE -ne 0) { Fail "origin/main 不是本地 HEAD 的祖先，拒绝推送（防止覆盖远端）" }
}

Write-Step "第 5 步：推送到 GitHub"
if ($DryRun) {
    Write-Host "[DryRun] git push origin main"
} elseif ($ahead -gt 0) {
    git push origin main
    if ($LASTEXITCODE -ne 0) { Fail "push 失败：确认已登录 GitHub 且有写入权限，然后重跑" }
    Write-Host "已推送 origin/main" -ForegroundColor Green
} else {
    Write-Host "没有需要推送的提交"
}

if ($Tag) {
    Write-Step "第 6 步：打版本 tag"
    if ($DryRun) {
        Write-Host "[DryRun] git tag -a $Tag 并 git push origin $Tag"
    } else {
        $exists = git tag -l $Tag
        if ($exists) {
            Write-Host "tag $Tag 已存在，跳过"
        } else {
            git tag -a $Tag -m $Tag
            git push origin $Tag
            if ($LASTEXITCODE -ne 0) { Fail "tag 推送失败" }
        }
    }
    Write-Host "用浏览器打开 Release 页填写说明：$web/releases/new?tag=$Tag&target=main" -ForegroundColor Green
}

Write-Step "验证"
if ($DryRun) {
    Write-Host "[DryRun] 跳过验证（不联网）"
    Write-Host "试运行通过：脚本可以正常启动执行。"
} else {
    git status -sb
    $localHead = (git rev-parse HEAD).Trim()
    $line = (git ls-remote origin refs/heads/main) -split '\s+' | Where-Object { $_ }
    $remoteHead = if ($line) { $line[0] } else { "" }
    Write-Host "本地 HEAD：$localHead"
    Write-Host "远端 main：$remoteHead"
    if ($localHead -ne $remoteHead) { Fail "本地与远端不一致，请检查推送结果" }
    Write-Host "完成：本地与 GitHub 已同步。" -ForegroundColor Green
}
