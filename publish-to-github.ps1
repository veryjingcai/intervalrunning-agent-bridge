param([switch]$DryRun)

# 桥接服务一键发布脚本（保存为 UTF-8 BOM，Windows PowerShell 5.1 可直接运行）
# 普通用法：.\publish-to-github.ps1        （打开浏览器建仓库、登录并推送）
# 试运行：  .\publish-to-github.ps1 -DryRun （不联网，只检查脚本能否正常启动执行）

$ErrorActionPreference = "Stop"

function Write-Step($text) { Write-Host "== $text ==" -ForegroundColor Cyan }

if ($DryRun) {
    Write-Step "试运行模式：不打开浏览器、不推送、不改 App"
    $repoUrl = "https://github.com/example/intervalrunning-agent-bridge"
} else {
    Write-Step "第 1 步：在浏览器打开 GitHub 新建仓库页"
    Write-Host "仓库名建议：intervalrunning-agent-bridge；可见性选 Public；不要勾选任何初始化文件。"
    Start-Process "https://github.com/new?name=intervalrunning-agent-bridge"
    $repoUrl = Read-Host "创建完成后，输入仓库地址（如 https://github.com/你的用户名/intervalrunning-agent-bridge）"
}

if ([string]::IsNullOrWhiteSpace($repoUrl)) {
    Write-Host "未输入仓库地址，已取消。" -ForegroundColor Yellow
    exit 1
}
if (-not $repoUrl.EndsWith(".git")) { $repoUrl = "$repoUrl.git" }

if (-not $DryRun) {
    Write-Step "第 2 步：初始化并推送（首次推送会弹出浏览器登录 GitHub）"
    git init
    git add bridge.py README.md .gitignore
    git commit -m "intervalrunning 智能体桥接服务 v1"
    git branch -M main
    git remote add origin $repoUrl 2>$null
    git remote set-url origin $repoUrl
    git push -u origin main
    if ($LASTEXITCODE -ne 0) {
        Write-Host "推送失败：请确认仓库已创建且为空，或检查登录后重跑本脚本。" -ForegroundColor Red
        exit 1
    }
}

$web = $repoUrl -replace "\.git$", ""
$download = "$web/releases/latest"

if (-not $DryRun) {
    Write-Step "第 3 步：发布 Release（可选但推荐）"
    Write-Host "打开 $web/releases/new ，Tag 填 v1，附件可上传本目录的 bridge.py。发布后下载链接固定为："
    Write-Host $download -ForegroundColor Green
}

Write-Step "第 4 步：把下载链接写进 App"
$appFile = "D:\Android\intervalrunning\app\src\main\java\com\jingcai\intervalrunning\ui\settings\AgentLinkCard.kt"
if ($DryRun) {
    Write-Host "试运行：跳过 App 修改。"
} elseif (Test-Path $appFile) {
    $content = [System.IO.File]::ReadAllText($appFile)
    $placeholder = "https://github.com/REPLACE-WITH-YOUR-REPO/intervalrunning-agent-bridge"
    $updated = $content.Replace($placeholder, $web)
    if ($updated -ne $content) {
        # Kotlin 源码不带 BOM 写回，避免编译器告警
        [System.IO.File]::WriteAllText($appFile, $updated, (New-Object System.Text.UTF8Encoding($false)))
        Write-Host "已把 App 里的下载链接更新为：$web" -ForegroundColor Green
        Write-Host "请重新构建安装 App（或在 CodeBuddy 里说一声），弹窗里的链接即正式地址。"
    } else {
        Write-Host "App 里的链接已是最新，无需修改。"
    }
} else {
    Write-Host "未找到 App 源码文件，请手动把下载链接填进 AgentLinkCard.kt 的 BRIDGE_DOWNLOAD_URL。" -ForegroundColor Yellow
}

Write-Step "完成"
if ($DryRun) { Write-Host "试运行通过：脚本可以正常启动执行。" -ForegroundColor Green }
