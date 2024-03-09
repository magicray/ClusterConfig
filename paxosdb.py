import os
import sys
import gzip
import time
import json
import asyncio
import sqlite3
import logging
import httprpc
import hashlib
import argparse


def path(db, create=False):
    db = hashlib.sha256(db.encode()).hexdigest()
    return os.path.join('paxosdb', db[0:3], db[3:6], db + '.sqlite3')


async def read(ctx, key=None):
    sub = ctx['subject']
    db = path(sub)
    if not os.path.isfile(db):
        raise Exception('NOT_INITIALIZED')

    db = sqlite3.connect(db)
    try:
        if key is None:
            # All keys
            return dict(db=sub, keys=db.execute(
                '''select key, version from paxos
                   where accepted_seq > 0
                ''').fetchall())

        else:
            # Most recent version of this key
            result = db.execute('''select version, value from paxos
                                   where key=? and accepted_seq > 0
                                   order by version desc limit 1
                                ''', [key]).fetchone()
            version, value = result if result else (None, b'')
            return dict(db=sub, key=key, version=version, value=value)
    finally:
        db.close()


async def paxos_server(ctx, key, version, seq, octets=None):
    seq = int(seq)
    version = int(version)

    proposal_time = seq / (10**9)
    if time.time() > proposal_time + 10 or time.time() < proposal_time - 10:
        # For liveness - out of sync clocks can block further rounds
        raise Exception('CLOCKS_OUT_OF_SYNC')

    db = path(ctx['subject'])
    os.makedirs(os.path.dirname(db), exist_ok=True)

    db = sqlite3.connect(db)
    try:
        db.execute('''create table if not exists paxos(
                          key          text,
                          version      int,
                          promised_seq int,
                          accepted_seq int,
                          value        blob,
                          primary key(key, version)
                      )''')

        db.execute('insert or ignore into paxos values(?,?,0,0,null)',
                   [key, version])

        if octets is None:
            # Paxos PROMISE - Block stale writers and return the most recent
            # accepted value. Client will propose the most recent across
            # servers in the accept phase
            promised_seq, accepted_seq, value = db.execute(
                '''select promised_seq, accepted_seq, value
                   from paxos where key=? and version=?
                ''', [key, version]).fetchone()

            if seq > promised_seq:
                db.execute('''update paxos set promised_seq=?
                              where key=? and version=?
                           ''', [seq, key, version])
                db.commit()

                # CRUX of the paxos protocol - return the accepted value
                return dict(accepted_seq=accepted_seq, value=value)
        else:
            # Paxos ACCEPT - Client has sent the most recent value from the
            # promise phase.
            promised_seq = db.execute(
                'select promised_seq from paxos where key=? and version=?',
                [key, version]).fetchone()[0]

            if seq >= promised_seq:
                db.execute('''update paxos
                              set promised_seq=?, accepted_seq=?, value=?
                              where key=? and version=?
                           ''', [seq, seq, octets, key, version])

                # Delete older version of this key.
                # This is unrelated to and does not impact Paxos steps.
                db.execute('''delete from paxos where key=? and version < (
                                  select max(version) from paxos
                                  where key=? and accepted_seq > 0)
                           ''',
                           [key, key])

                return db.commit()
    finally:
        db.rollback()
        db.close()

    raise Exception(f'STALE_PROPOSAL_SEQ {key}:{version} {seq}')


class Client():
    def __init__(self, cacert, cert, servers):
        self.client = httprpc.Client(cacert, cert, servers)

    async def rpc(self, resource, octets=b''):
        res = await self.client.cluster(resource, octets)
        result = list()

        exceptions = list()
        for s, r in zip(self.client.conns.keys(), res):
            if isinstance(r, Exception):
                exceptions.append(f'\n-{s}\n{r}')
            else:
                result.append(r)

        if len(result) < self.client.quorum:
            raise Exception('\n'.join(exceptions))

        return result

    async def paxos_client(self, key, version, obj=b''):
        seq = int(time.time()*10**9)  # Current microsecond is a good enough
        url = f'paxos/key/{key}/version/{version}/seq/{seq}'

        if obj != b'':
            # value to be set should always be json serializable
            octets = gzip.compress(json.dumps(obj).encode())

        # Paxos PROMISE phase - block stale writers and finalize the proposal
        accepted_seq = 0
        for v in await self.rpc(url):
            # CRUX of the paxos protocol - Find the most recent accepted value
            if v['accepted_seq'] > accepted_seq:
                accepted_seq, octets = v['accepted_seq'], v['value']

        # Paxos ACCEPT phase - propose the value found above
        # This may fail but we don't check the return value as we can't take
        # any corrective action. Entire process must be retried.
        await self.rpc(url, octets)

    async def get(self, key=None):
        # Return the latest version of accepted keys
        if key is None:
            keys = dict()
            for res in await self.rpc('read'):
                for key, version in res['keys']:
                    if key not in keys or version > keys[key]:
                        keys[key] = version

            return dict(db=res['db'], keys=keys)

        # Verify if the value for a key-version has been finalized
        for i in range(self.client.quorum):
            res = await self.rpc(f'read/key/{key}')

            # version,value pair returned by all the nodes must be same
            if all([res[0] == v for v in res]):
                val = res[0].pop('value')

                if res[0]['version'] is not None:
                    res[0]['value'] = json.loads(gzip.decompress(val).decode())

                return res[0]

            # All the nodes do not agree on a version-value for this key yet.
            # Start a paxos round to build the consensus.
            versions = [v['version'] for v in res if v['version'] is not None]
            await self.paxos_client(key, max(versions))

    async def put(self, key, version, obj):
        # Run a paxos round to build consensus
        await self.paxos_client(key, version, obj)

        # Paxos guarantees that the value for the returned version is now
        # final and would not change under any condition.
        return await self.get(key)


async def get(ctx, key=None):
    return await Client(G.cert, G.cert, G.servers).get(key)


async def put(ctx, key, version, obj):
    return await Client(G.cert, G.cert, G.servers).put(key, version, obj)


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    P = argparse.ArgumentParser()
    P.add_argument('--port', type=int, help='port number for server')
    P.add_argument('--cert', help='certificate path')
    P.add_argument('--cacert', help='ca certificate path')
    P.add_argument('--servers', help='comma separated list of server ip:port')
    P.add_argument('--key', help='key for get/put')
    P.add_argument('--version', type=int, help='version for put')
    G = P.parse_args()

    if G.cacert and G.cert and G.port:
        # Start the server
        httprpc.run(G.port, dict(get=get, put=put, read=read,
                                 paxos=paxos_server),
                    cacert=G.cacert, cert=G.cert)

    elif G.cacert and G.cert and G.servers and G.port is None:
        if G.version is not None:
            # Write the value for a key, version
            coro = put({}, G.key, G.version, json.loads(sys.stdin.read()))

        else:
            # Read the latest version of a key
            coro = get({}, G.key)

        print(json.dumps(asyncio.run(coro), sort_keys=True, indent=4))
    else:
        P.print_help()
        exit(1)
