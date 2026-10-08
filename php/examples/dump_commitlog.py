#!/usr/bin/env python3
"""离线解析 RocketMQ 5.x commitlog，打印每条消息的 reconsumeTimes / topic / properties。

用途：POP S2 重投 recon=0 的取证——直接看 broker 写进 commitlog 的 reconsumeTimes
字段是 0 还是 1（reviveRetry 的 suspend 分支会保持原值）。

用法：python3 dump_commitlog.py <store/commitlog 目录> [--filter ORIGIN_GROUP|%RETRY%]
"""
import struct
import sys
from pathlib import Path

MAGIC = (-626843481, -626843477)  # MESSAGE_MAGIC_CODE / V2


def parse(path: Path, flt: str | None):
    data = path.read_bytes()
    out = []
    pos = 0
    n = len(data)
    while pos + 4 <= n:
        (total,) = struct.unpack_from('>i', data, pos)
        if total <= 0 or pos + total > n:
            break  # 文件尾 / 预分配零区（total 含自身 4B）
        msg = data[pos: pos + total]
        pos += total
        magic = struct.unpack_from('>i', msg, 4)[0]
        if magic not in MAGIC:
            continue
        qoff, poff = struct.unpack_from('>qq', msg, 20)
        sysflag = struct.unpack_from('>i', msg, 36)[0]
        born_v6 = bool(sysflag & 0x2)
        store_v6 = bool(sysflag & 0x4)
        off = 40                            # bornTimestamp（含 totalLen 头的布局）
        off += 8 + (20 if born_v6 else 8)   # bornTs + bornHost
        off += 8 + (20 if store_v6 else 8)  # storeTs + storeHost
        recon = struct.unpack_from('>i', msg, off)[0]
        off += 4 + 8                        # reconsumeTimes + prepTxOffset
        try:
            (blen,) = struct.unpack_from('>i', msg, off)
            off += 4
            body = msg[off: off + max(0, blen)]
            off += blen
            use_v2 = magic == MAGIC[1]
            if use_v2:
                (tlen,) = struct.unpack_from('>H', msg, off)
                off += 2
            else:
                tlen = msg[off]
                off += 1
            topic = msg[off: off + tlen].decode(errors='replace')
            off += tlen
            (plen,) = struct.unpack_from('>H', msg, off)
            off += 2
            props = msg[off: off + plen].decode(errors='replace')
        except (struct.error, IndexError):
            continue
        out.append((topic, recon, qoff, body, props))
    if flt:
        out = [r for r in out if flt in r[0] or flt in r[4]]
    return out


def main() -> None:
    commit_dir = Path(sys.argv[1])
    flt = sys.argv[3] if len(sys.argv) > 3 else None
    files = sorted(commit_dir.glob('*'))
    print(f'{len(files)} commitlog file(s) in {commit_dir}')
    n = 0
    for p in files:
        for topic, recon, qoff, body, props in parse(p, flt):
            n += 1
            print(f'#{n} topic={topic} recon={recon} qoff={qoff} body={body[:32]!r}')
            print(f'    props={props[:200]}')
    print(f'--- {n} message(s) ---')


if __name__ == '__main__':
    main()
