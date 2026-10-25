#!/usr/bin/env bash
# impl-06c · 09：观测器日志的内存/压力形态汇总（对撞灌数期 13:49:46–14:20:03 vs 沉降后）
set -uo pipefail
cd "$(dirname "$0")"
python3 - 04-observer.log <<'PY'
import sys
rows = []
with open(sys.argv[1]) as f:
    hdr = f.readline().rstrip('\n').split('\t')
    for line in f:
        p = line.rstrip('\n').split('\t')
        if len(p) >= 14 and ':' in p[0]:
            rows.append(dict(zip(hdr, p)))

def gi(s):
    try: return float(s.split('GiB')[0])
    except Exception: return None

def fnum(s):
    try: return float(s)
    except Exception: return None

LOAD = ('13:49', '14:20')  # 灌数窗（含边界外延 1 个采样）

def phase(ts):
    return 'load' if LOAD[0] <= ts[:5] <= LOAD[1] else 'other'

load_rows = [r for r in rows if phase(r['ts']) == 'load']

be = [(r['ts'], gi(r['be_mem'])) for r in load_rows if gi(r['be_mem']) is not None]
alloc = [(r['ts'], fnum(r['alloc_mb'])) for r in load_rows if fnum(r['alloc_mb']) is not None]
pswpo = [(r['ts'], fnum(r['pswpout'])) for r in rows if fnum(r['pswpout']) is not None]
fq = [(r['ts'], fnum(r['flush_q'])) for r in load_rows if fnum(r['flush_q']) is not None]
fa = [(r['ts'], fnum(r['flush_act'])) for r in load_rows if fnum(r['flush_act']) is not None]
sc = [(r['ts'], fnum(r['score'])) for r in load_rows if fnum(r['score']) is not None]
ssc = [(r['ts'], fnum(r['size_score'])) for r in load_rows if fnum(r['size_score']) is not None]

print(f'samples: total={len(rows)} load_window={len(load_rows)}')
print(f'be_mem peak: {max(be, key=lambda x: x[1])} GiB / 3GiB limit')
print(f'be_mem peak top5: {sorted(be, key=lambda x: -x[1])[:5]}')
print(f'alloc peak: {max(alloc, key=lambda x: x[1])} B = {max(alloc, key=lambda x: x[1])[1]/2**30:.3f} GiB')
print(f'flush queue peak: {max(fq, key=lambda x: x[1]) if fq else None}')
print(f'flush active peak: {max(fa, key=lambda x: x[1]) if fa else None}')
print(f'compaction score peak: {max(sc, key=lambda x: x[1]) if sc else None}')
print(f'size_based score peak: {max(ssc, key=lambda x: x[1]) if ssc else None}')
print(f'pswpout: start={pswpo[0]} peak={max(pswpo, key=lambda x: x[1])} end={pswpo[-1]}')
dupre = [r for r in rows if r['ts'] < '13:49']
print(f'du storage: pre-load={dupre[-1]["db_kb"] if dupre else "?"} KB, final={rows[-1]["db_kb"]} KB')
PY
echo "=== 09 DONE ==="
