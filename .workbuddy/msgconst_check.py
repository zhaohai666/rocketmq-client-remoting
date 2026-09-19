import re, os

J = 'D:/zhaohai666-rocketmq/rocketmq/common/src/main/java/org/apache/rocketmq/common/message/MessageConst.java'
s = open(J, encoding='utf-8', errors='ignore').read()
pairs = re.findall(r'String\s+([A-Z_0-9]+)\s*=\s*"([^"]+)"', s)
# keep only the wire-visible property keys (values that are not java package-ish strings)
pairs = [(n, v) for n, v in pairs if not v.startswith('org.apache') and v]

files = {
    'cpp': ['cpp/include', 'cpp/src'],
    'dotnet': ['dotnet/src/RocketMQ.Client/Common/MessageConst.cs', 'dotnet/src'],
    'python': ['python/rocketmq/common/message_const.py', 'python/rocketmq'],
    'rust': ['rust/src/common/message_const.rs', 'rust/src'],
}


def blob(paths):
    out = []
    for p in paths:
        if os.path.isdir(p):
            for dp, _, fs in os.walk(p):
                for f in fs:
                    if re.search(r'\.(h|hpp|cpp|cs|py|rs)$', f):
                        out.append(open(os.path.join(dp, f), encoding='utf-8', errors='ignore').read())
        else:
            out.append(open(p, encoding='utf-8', errors='ignore').read())
    return '\n'.join(out)


print(f'Java MessageConst: {len(pairs) if False else len(pairs)} wire constants\n')
for lang, paths in files.items():
    t = blob(paths)
    absent = [f'{n}="{v}"' for n, v in pairs if f'"{v}"' not in t and f"'{v}'" not in t]
    print(f'{lang}: {len(pairs) - len(absent)}/{len(pairs)} wire keys present | missing {len(absent)}')
    for a in absent:
        print('    ' + a)
    print()
