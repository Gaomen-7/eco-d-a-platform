#!/usr/bin/env python3
"""compose 配置契约断言 —— HLD §4.2 的「脚本断言」落点。

为什么需要它：`docker compose config` 能**列出** mem_limit，但**不报缺**
（HLD §4.2 实测结论），所以「逐服务不允许缺省」这条判据必须脚本化。
同时断言端口绑定（REQ-F-ORCH-06）。

退出码：0 = 全部通过；1 = 有违规（供 make / CI 判）。
"""

from __future__ import annotations

import json
import subprocess
import sys


def main() -> int:
    raw = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    cfg = json.loads(raw)

    bad: list[str] = []
    resident = 0

    for name, svc in sorted(cfg.get("services", {}).items()):
        mem = svc.get("mem_limit")
        swap = svc.get("memswap_limit")

        # ① 逐服务必须显式写 mem_limit（缺省即违规）
        if not mem:
            bad.append(f"{name}: 缺 mem_limit")
        # ② 必须同时写 memswap_limit，且与 mem_limit 相等
        #    （官方：只设 memory 时容器可借等量 swap ⇒ 预算有渗漏点）
        elif not swap:
            bad.append(f"{name}: 缺 memswap_limit（容器可借等量 swap）")
        elif mem != swap:
            bad.append(f"{name}: memswap_limit({swap}) != mem_limit({mem})")

        # ③ 常驻预算：排除 profile 隔离的按需服务
        if mem and not svc.get("profiles"):
            resident += int(mem)

        # ④ 端口必须绑 127.0.0.1（不写 host IP 会绑 0.0.0.0 并绕过宿主防火墙）
        for port in svc.get("ports", []):
            if port.get("host_ip") != "127.0.0.1":
                bad.append(
                    f"{name}: 端口 {port.get('published')} 绑到 "
                    f"{port.get('host_ip') or '0.0.0.0'}（须 127.0.0.1）"
                )

    ceiling_mib = 11962  # WSL 实测 MemTotal（FSR U1 / HLD §4.2）
    print(f"  常驻上限合计 = {resident / 1024**3:.2f} GiB")
    print(f"  天花板       = {ceiling_mib / 1024:.2f} GiB")
    print(f"  余量         = {(ceiling_mib * 1024**2 - resident) / 1024**3:.2f} GiB")

    if bad:
        print("  VIOLATIONS:")
        for b in bad:
            print(f"    - {b}")
        return 1

    print("  config-check: PASS（逐服务 limits + 127.0.0.1 绑定）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
