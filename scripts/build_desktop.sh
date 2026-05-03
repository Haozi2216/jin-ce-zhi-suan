#!/usr/bin/env bash
# ============================================================
#  桌面端打包脚本（macOS / Linux）
#  使用 PyInstaller 将项目打包为桌面程序
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

step() { printf '\033[32m[build]\033[0m %s\n' "$1"; }
warn() { printf '\033[33m[build]\033[0m %s\n' "$1" >&2; }
die()  { printf '\033[31m[build]\033[0m %s\n' "$1" >&2; exit 1; }

# 检测 Python
detect_python() {
  for cand in python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then echo "$cand"; return; fi
  done
  return 1
}

PYTHON="$(detect_python || die "未检测到 Python，请先安装")"
step "使用解释器: $PYTHON"

step "安装 PyInstaller..."
"$PYTHON" -m pip install pyinstaller -q

# 确保 macOS 菜单栏托盘依赖存在（pystray + Pillow + pyobjc）
if [[ "$(uname)" == "Darwin" ]]; then
  step "安装托盘依赖（pystray / Pillow / pyobjc）..."
  "$PYTHON" -m pip install pystray Pillow pyobjc -q
fi

step "开始打包（onedir 模式，首次约 5-10 分钟）..."

if [[ -d "dist" ]]; then
  step "清理旧构建产物..."
  rm -rf "dist/金策智算" 2>/dev/null || true
  rm -rf "dist/金策智算.app" 2>/dev/null || true
fi

"$PYTHON" -m PyInstaller desktop.spec --clean

step "打包完成！"
echo ""
echo "运行程序："
if [[ "$(uname)" == "Darwin" ]]; then
  echo "  open dist/金策智算.app"
  echo "  或双击 dist/金策智算.app"
else
  echo "  ./dist/金策智算/金策智算"
fi
