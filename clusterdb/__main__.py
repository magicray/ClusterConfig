import sys
import json
import asyncio
import argparse
import clusterdb


async def get(G):
    return await G.client.get(G.key)


async def put(G):
    return await G.client.put(G.key, G.version, sys.stdin.read().strip())


async def keys(G):
    return await G.client.keys()


if '__main__' == __name__:
    G = argparse.ArgumentParser()
    G.add_argument('--key', help='key')
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G.add_argument('--version', help='version')
    G = G.parse_args()

    G.client = clusterdb.Client(G.cacert, G.cert, G.servers)

    if G.version:
        result = asyncio.run(put(G))
    elif G.key:
        result = asyncio.run(get(G))
    else:
        result = asyncio.run(keys(G))

    print(json.dumps(result, sort_keys=True, indent=4))
