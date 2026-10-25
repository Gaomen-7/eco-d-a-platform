#!/usr/bin/env bash
# impl-06c · 10c v2：memtable 限流形态精查（python 单遍解析，避免 awk 转义问题）
set -uo pipefail
cd "$(dirname "$0")"

docker exec eco-doris-be sh -c "grep -aE 'I20261025 (1[34]|15):' /opt/apache-doris/be/log/be.INFO 2>/dev/null" > /tmp/impl06c-window.log
python3 - /tmp/impl06c-window.log <<'PY'
import re, sys
LOAD_START, LOAD_END = '13:49:46', '14:20:03'
COMP_END = '15:01:00'
waited, hard = [], []
n_lines = 0
with open(sys.argv[1], errors='replace') as f:
    for line in f:
        n_lines += 1
        m = re.search(r'I20261025 (\d\d:\d\d:\d\d)\.\d+', line)
        if not m:
            continue
        t = m.group(1)
        if 'waited' in line and 'memtable memory limit' in line:
            waited.append((t, line))
        if 'reached hard limit' in line:
            hard.append((t, line))

def to_ms(s):
    s = re.search(r'waited (.+?) for', s).group(1)
    total = 0
    for num, unit in re.findall(r'(\d+)(ms|s)', s):
        total += int(num) * (1 if unit == 'ms' else 1000)
    return total

def loadmem_kb(l):
    m = re.search(r'load mem: ([\d.]+) (\wB)', l)
    if not m:
        return 0.0
    v, u = float(m.group(1)), m.group(2)
    return v * {'KB': 1, 'MB': 1024, 'GB': 1024 * 1024}[u]

lw = [(t, l) for t, l in waited if LOAD_START <= t <= LOAD_END]
cw = [(t, l) for t, l in waited if LOAD_END < t <= COMP_END]
lh = [(t, l) for t, l in hard if LOAD_START <= t <= COMP_END]

print(f'window lines: {n_lines}')
print(f'waited lines: total={len(waited)} load_window={len(lw)} compaction_window={len(cw)}')
if lw:
    ws = sorted(to_ms(l) for t, l in lw)
    print(f'  load-window wait ms: N={len(ws)} min={ws[0]} p50={ws[len(ws)//2]} p90={ws[int(len(ws)*0.9)]} max={ws[-1]}')
    print(f'  first={lw[0][0]} last={lw[-1][0]}')
    lm = max(lw, key=lambda x: loadmem_kb(x[1]))
    m = re.search(r'load mem: ([\d.]+ \wB)', lm[1])
    print(f'  load mem peak in waited lines: {m.group(1) if m else "NA"} at {lm[0]}')
    pm = max(lw, key=lambda x: float(re.search(r"process memory used ([\d.]+) GB", x[1]).group(1)))
    print(f'  process mem peak in waited lines: {re.search(r"process memory used ([\d.]+ GB)", pm[1]).group(1)} at {pm[0]}')
print(f'reached hard limit: total={len(hard)} in [load..compaction] window={len(lh)}')
if lh:
    print(f'  first={lh[0][0]} last={lh[-1][0]}')
    nz = [(t, re.search(r'load mem: ([\d.]+ \wB)', l).group(1)) for t, l in lh]
    big = [x for x in nz if not x[1].startswith(('0.', '0 '))]
    print(f'  non-tiny load mem entries: {len(big)}; top5: {sorted(big, key=lambda x: x[1])[-5:]}')
PY
echo "=== 10C-v2 DONE ==="
