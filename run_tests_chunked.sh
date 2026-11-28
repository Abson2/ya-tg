#!/usr/bin/env bash
# 分块跑全量：每块 N 个测试文件，独立进程（规避本机内存不足导致整进程被杀）。
# 汇总输出形如 "NNNN passed"，与单进程 pytest 的收尾行同口径，供发版脚本读取。
set -u
cd "$(dirname "$0")" || exit 1
PY="${PY:-/tmp/yatg-venv/bin/python}"
CHUNK="${CHUNK:-10}"
OUT=/tmp/ya-tg-final-test.log
: > "$OUT"
ls tests/test_*.py > /tmp/_all_tests.txt
rm -f /tmp/_tchunk_*
split -l "$CHUNK" /tmp/_all_tests.txt /tmp/_tchunk_

TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_ERR=0
FAILED_FILES=""

for f in /tmp/_tchunk_*; do
  files=$(tr '\n' ' ' < "$f")
  log=$(mktemp)
  PYTHONPATH=backend "$PY" -m pytest $files -q > "$log" 2>&1
  rc=$?
  tail -1 "$log" >> "$OUT"
  p=$(grep -oE '[0-9]+ passed' "$log" | tail -1 | cut -d' ' -f1)
  x=$(grep -oE '[0-9]+ failed' "$log" | tail -1 | cut -d' ' -f1)
  e=$(grep -oE '[0-9]+ error' "$log" | tail -1 | cut -d' ' -f1)
  TOTAL_PASS=$((TOTAL_PASS + ${p:-0}))
  TOTAL_FAIL=$((TOTAL_FAIL + ${x:-0}))
  TOTAL_ERR=$((TOTAL_ERR + ${e:-0}))
  echo "  chunk $(basename "$f"): rc=$rc passed=${p:-0} failed=${x:-0} error=${e:-0}" >> "$OUT"
  if [ "$rc" -ne 0 ]; then
    FAILED_FILES="$FAILED_FILES $(basename "$f")"
    # 把失败块的关键摘要留档，便于定位
    echo "--- FAILED CHUNK $(basename "$f") ---" >> "$OUT"
    grep -E "^(FAILED|ERROR)" "$log" | head -40 >> "$OUT"
  fi
  rm -f "$log"
done

echo "" >> "$OUT"
echo "===== 汇总 =====" >> "$OUT"
echo "${TOTAL_PASS} passed, ${TOTAL_FAIL} failed, ${TOTAL_ERR} error" >> "$OUT"
if [ "$TOTAL_FAIL" -eq 0 ] && [ "$TOTAL_ERR" -eq 0 ]; then
  echo "ALL_GREEN" >> "$OUT"
  exit 0
else
  echo "HAS_FAILURES chunks:$FAILED_FILES" >> "$OUT"
  exit 1
fi
