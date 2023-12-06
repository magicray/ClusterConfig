import sys
import json
import confdb
import asyncio
import argparse


async def main(G):
    client = confdb.Client(G.cacert, G.cert, G.servers)
    result = await client.put(G.key, G.version, sys.stdin.read().strip())
    print(json.dumps(result, sort_keys=True, indent=4))


if '__main__' == __name__:
    G = argparse.ArgumentParser()
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G.add_argument('--key', help='key')
    G.add_argument('--version', help='version')
    G = G.parse_args()

    asyncio.run(main(G))
