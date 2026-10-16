#!/usr/bin/env bash
# 预推送校验：密钥扫描 + 只暂存源码 + 复核暂存内容
# 用途：英文化改动提交前，确保不误提交构建产物、不泄露密钥。
# 用法：bash scripts/prepush_check.sh [--push]
#   不带 --push：只做校验与暂存，打印将要提交的文件清单
#   带 --push   ：校验通过后执行 commit + push --force-with-lease origin <branch>
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
DO_PUSH=0
[ "${1:-}" = "--push" ] && DO_PUSH=1

echo "=== [1/5] 分支 / 远程 ==="
echo "branch : $BRANCH"
git remote -v | head -2

echo
echo "=== [2/5] 密钥扫描（仅针对新增/改动的行）==="
# 只扫 diff 的 + 行，避免历史代码里的示例常量误报
SECRET_RE='(-----BEGIN [A-Z ]*PRIVATE KEY-----)|([A-Za-z0-9_]*(secret|passwd|password|token|access[_-]?key)[A-Za-z0-9_]*[[:space:]]*[:=][[:space:]]*["'"'"'][^"'"'"']{12,})|(https?://[^[:space:]]*:[^[:space:]@]*@)|(gh[pousr]_[A-Za-z0-9]{20,})|(xox[baprs]-[A-Za-z0-9-]{10,})|(sk-[A-Za-z0-9]{20,})'
HITS="$(git diff -U0 | grep -E '^\+' | grep -vE '^\+\+\+' | grep -nEi "$SECRET_RE" || true)"
if [ -n "$HITS" ]; then
  echo "!!! 发现疑似真实密钥，中止推送："
  echo "$HITS" | head -20
  exit 2
fi
echo "OK：新增行中未发现真实密钥（代码标识符如 accessKey/secretKey 不视为密钥）"

echo
echo "=== [3/5] 暂存源码（排除构建产物与 lock 抖动）==="
git add -u
git reset -q -- '**/bin/**' '**/obj/**' '**/target/**' '*.dll' '*.pdb' '*.o' '*.so' \
              '*.pyc' '*.pyo' '__pycache__' '*.class' '*.jar' '*.lock' 2>/dev/null || true

echo
echo "=== [4/5] 暂存内容复核（必须是源码/文档，不得含二进制）==="
STAGED="$(git diff --cached --name-only)"
if [ -z "$STAGED" ]; then
  echo "!! 暂存区为空，没有可提交的改动"
  exit 3
fi
echo "已暂存文件数: $(echo "$STAGED" | wc -l | tr -d ' ')"

# 任何非源码扩展名的暂存文件都视为可疑
SUSPECT="$(echo "$STAGED" | grep -vE '\.(py|php|cs|ts|js|go|rs|cpp|h|hpp|md|txt|json|ya?ml|sh|cmake|toml|xml|proto|gitignore|props|csproj)$' || true)"
if [ -n "$SUSPECT" ]; then
  echo "!!! 以下暂存文件不是纯源码/文档扩展名，请人工确认："
  echo "$SUSPECT" | head -20
  exit 4
fi

# 二进制内容检测
BINDIFF="$(git diff --cached --numstat | awk '$1=="-" || $2=="-" {print $3}')"
if [ -n "$BINDIFF" ]; then
  echo "!!! 暂存区含二进制内容，中止："
  echo "$BINDIFF" | head -20
  exit 5
fi

echo "按扩展名分布："
echo "$STAGED" | sed -n 's/.*\.\([A-Za-z0-9]*\)$/\1/p' | sort | uniq -c | sort -rn
echo "按顶层目录分布："
echo "$STAGED" | awk -F/ '{print $1}' | sort | uniq -c | sort -rn

echo
echo "=== [5/5] 结论 ==="
if [ "$DO_PUSH" = "1" ]; then
  echo "校验通过，执行 commit + push"
  git commit -q -m "${COMMIT_MSG:-docs(i18n): translate comments, docstrings and log/error messages to English across all clients}" || exit 6
  echo "commit: $(git rev-parse --short HEAD)"
  git push --force-with-lease origin "$BRANCH" || exit 7
  echo "PUSH OK -> origin/$BRANCH"
else
  echo "校验通过（未推送）。加 --push 参数可执行提交与推送。"
  echo "提示：可用 COMMIT_MSG='...' bash scripts/prepush_check.sh --push 自定义提交信息"
fi
