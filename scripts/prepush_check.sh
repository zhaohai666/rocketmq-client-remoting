#!/usr/bin/env bash
# Pre-push check: secret scan + stage sources only + review what got staged.
# Purpose: before committing an Englishization change, make sure build outputs
# are never committed and no key leaks.
# Usage: bash scripts/prepush_check.sh [--push]
#   without --push: only check and stage, print the list of files to commit
#   with --push   : after the checks pass, run commit + push --force-with-lease origin <branch>
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
DO_PUSH=0
[ "${1:-}" = "--push" ] && DO_PUSH=1

echo "=== [1/5] branch / remote ==="
echo "branch : $BRANCH"
git remote -v | head -2

echo
echo "=== [2/5] secret scan (added/changed lines only) ==="
# Scan only the + lines of the diff, so sample constants in old code do not fire.
SECRET_RE='(-----BEGIN [A-Z ]*PRIVATE KEY-----)|([A-Za-z0-9_]*(secret|passwd|password|token|access[_-]?key)[A-Za-z0-9_]*[[:space:]]*[:=][[:space:]]*["'"'"'][^"'"'"']{12,})|(https?://[^[:space:]]*:[^[:space:]@]*@)|(gh[pousr]_[A-Za-z0-9]{20,})|(xox[baprs]-[A-Za-z0-9-]{10,})|(sk-[A-Za-z0-9]{20,})'
HITS="$(git diff -U0 | grep -E '^\+' | grep -vE '^\+\+\+' | grep -nEi "$SECRET_RE" || true)"
if [ -n "$HITS" ]; then
  echo "!!! possible real secrets found, aborting the push:"
  echo "$HITS" | head -20
  exit 2
fi
echo "OK: no real secret in the added lines (identifiers like accessKey/secretKey are not secrets)"

echo
echo "=== [3/5] stage sources (build outputs and lock churn excluded) ==="
git add -u
git reset -q -- '**/bin/**' '**/obj/**' '**/target/**' '*.dll' '*.pdb' '*.o' '*.so' \
              '*.pyc' '*.pyo' '__pycache__' '*.class' '*.jar' '*.lock' 2>/dev/null || true

echo
echo "=== [4/5] review of the staged content (sources/docs only, no binaries) ==="
STAGED="$(git diff --cached --name-only)"
if [ -z "$STAGED" ]; then
  echo "!! staging area is empty, nothing to commit"
  exit 3
fi
echo "staged file count: $(echo "$STAGED" | wc -l | tr -d ' ')"

# Any staged file without a source/doc extension counts as suspicious
SUSPECT="$(echo "$STAGED" | grep -vE '\.(py|php|cs|ts|js|go|rs|cpp|h|hpp|md|txt|json|ya?ml|sh|cmake|toml|xml|proto|gitignore|props|csproj)$' || true)"
if [ -n "$SUSPECT" ]; then
  echo "!!! these staged files are not plain source/doc extensions, check by hand:"
  echo "$SUSPECT" | head -20
  exit 4
fi

# binary content detection
BINDIFF="$(git diff --cached --numstat | awk '$1=="-" || $2=="-" {print $3}')"
if [ -n "$BINDIFF" ]; then
  echo "!!! the staging area contains binary content, aborting:"
  echo "$BINDIFF" | head -20
  exit 5
fi

echo "staged files by extension:"
echo "$STAGED" | sed -n 's/.*\.\([A-Za-z0-9]*\)$/\1/p' | sort | uniq -c | sort -rn
echo "staged files by top-level directory:"
echo "$STAGED" | awk -F/ '{print $1}' | sort | uniq -c | sort -rn

echo
echo "=== [5/5] conclusion ==="
if [ "$DO_PUSH" = "1" ]; then
  echo "checks passed, running commit + push"
  git commit -q -m "${COMMIT_MSG:-docs(i18n): translate comments, docstrings and log/error messages to English across all clients}" || exit 6
  echo "commit: $(git rev-parse --short HEAD)"
  git push --force-with-lease origin "$BRANCH" || exit 7
  echo "PUSH OK -> origin/$BRANCH"
else
  echo "checks passed (not pushed). Pass --push to commit and push."
  echo "hint: COMMIT_MSG='...' bash scripts/prepush_check.sh --push sets the commit message"
fi
