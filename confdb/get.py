import sys
import confdb
import asyncio
import logging
import argparse
from logging import critical as log


async def main(G):
    G.client = confdb.Client(G.cacert, G.cert, G.servers)
    result = await G.client.get(G.key)
    log(f"version : {result['version']}")
    sys.stdout.buffer.write(result['value'])

if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G.add_argument('--key', help='key')
    G = G.parse_args()

    asyncio.run(main(G))
