import re, os

root = 'D:/project/rocketmq-client-remoting'
java = 'D:/zhaohai666-rocketmq/rocketmq/client/src/main/java/org/apache/rocketmq/client'

codes = sorted(set(re.findall(r'RequestCode\.([A-Z_0-9]+)', open_java := ''.join(
    open(os.path.join(dp, f), encoding='utf-8', errors='ignore').read()
    for dp, _, fs in os.walk(java) for f in fs if f.endswith('.java')))))

targets = {
    'cpp': ['cpp/src', 'cpp/include'],
    'dotnet': ['dotnet/src'],
    'python': ['python/rocketmq'],
    'rust': ['rust/src'],
}
client_only = {
    'cpp': ['cpp/src/client'],
    'dotnet': ['dotnet/src/RocketMQ.Client/Client'],
    'python': ['python/rocketmq/client'],
    'rust': ['rust/src/client'],
}
skip = {'codes.h', 'codes.cpp', 'codes.cs', 'codes.py', 'codes.rs'}


def norm(s):
    return re.sub(r'[^a-z0-9]', '', s.lower())


def scan(dirs_by_lang):
    results = {}
    for lang, dirs in dirs_by_lang.items():
        chunks = []
        for d in dirs:
            for dp, _, fs in os.walk(os.path.join(root, d)):
                for f in fs:
                    if f.lower() in skip or not re.search(r'\.(h|hpp|cpp|cs|py|rs)$', f):
                        continue
                    chunks.append(open(os.path.join(dp, f), encoding='utf-8', errors='ignore').read())
        hay = norm('\n'.join(chunks))
        results[lang] = [c for c in codes if norm(c) not in hay]
    return results


for label, dirs in (('ALL SOURCE', targets), ('CLIENT LAYER ONLY', client_only)):
    results = scan(dirs)
    print(f'===== {label} =====')
    print(f'Java client/ uses {len(codes)} distinct RequestCodes')
    for lang, missing in results.items():
        print(f'  {lang}: {len(codes) - len(missing)}/{len(codes)} referenced | missing {len(missing)}')
    print()

results = scan(client_only)
for lang, missing in results.items():
    print(f'--- {lang} client-layer missing {len(missing)} ---')
    for i in range(0, len(missing), 6):
        print('   ' + ' '.join(missing[i:i + 6]))
    print()
