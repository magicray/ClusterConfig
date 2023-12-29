import sys
import json
import asyncio
import httprpc
import argparse
import clusterdb


async def get(ctx, key):
    return await G.client.get(key)


async def put(ctx, key, version, text):
    return await G.client.put(key, version, text)


async def keys(ctx):
    return await G.client.keys()


if '__main__' == __name__:
    G = argparse.ArgumentParser()
    G.add_argument('--db', help='db')
    G.add_argument('--key', help='key')
    G.add_argument('--secret', help='api secret')
    G.add_argument('--server', help='server ip:port')
    G.add_argument('--version', help='version')
    G = G.parse_args()

    G.client = clusterdb.Client(G.cacert, G.cert, G.servers)

    if G.port:
        httprpc.run(G.port, dict(get=get, put=put, keys=keys), ip='127.0.0.1')
    elif G.version:
        result = asyncio.run(put(None, G.key, G.version,
                                 sys.stdin.read().strip()))
    elif G.key:
        result = asyncio.run(get(None, G.key))
    else:
        result = asyncio.run(keys(None))

    print(json.dumps(result, sort_keys=True, indent=4))
