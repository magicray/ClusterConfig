import sys
import confdb
import asyncio
import logging
import argparse
from logging import critical as log


async def main(G):
    try:
        client = confdb.Client(G.cacert, G.cert, G.servers)
        log(await client.put(G.key, G.version, sys.stdin.buffer.read()))
    except Exception as e:
        log(e)
        exit(1)


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G.add_argument('--key', help='key')
    G.add_argument('--version', help='version')
    G = G.parse_args()

    asyncio.run(main(G))
