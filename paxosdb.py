import os
import sys
import gzip
import time
import json
import hmac
import uuid
import asyncio
import sqlite3
import logging
import httprpc
import hashlib
import argparse


def path(db, create=False):
    db = hashlib.sha256(db.encode()).hexdigest()
    return os.path.join('paxosdb', db[0:3], db[3:6], db + '.sqlite3')


async def read_server(ctx, db, key=None, version=None):
    db = path(db)
    if not os.path.isfile(db):
        raise Exception('NOT_INITIALIZED')

    db = sqlite3.connect(db)
    try:
        if key is None:
            # All keys
            return db.execute('select distinct key from paxos').fetchall()

        elif key is not None and version is None:
            # Most recent version of this key
            return db.execute('''select version, value from paxos
                                 where key=? and accepted_seq > 0
                                 order by version desc limit 1
                              ''', [key]).fetchone()
        elif key is not None and version is not None:
            # Value of the given key and version
            return db.execute('''select value from paxos
                                 where key=? and version = ?
                              ''', [key, int(version)]).fetchone()
    finally:
        db.close()


async def paxos_server(ctx, db, key, version, seq, retain, octets=None):
    seq = int(seq)
    retain = int(retain)
    version = int(version)

    proposal_time = seq / (10**9)
    if time.time() > proposal_time + 10 or time.time() < proposal_time - 10:
        # For liveness - out of sync clocks can block further rounds
        raise Exception('CLOCKS_OUT_OF_SYNC')

    if not ctx.get('subject', ''):
        raise Exception('TLS_AUTH_FAILED')

    if retain < 0:
        raise Exception('INVALID_RETAIN_VALUE')

    db = path(db)
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

                # This is unrelated to and does not impact Paxos steps.
                # Delete older version of this key.
                db.execute('delete from paxos where key=? and version < ?',
                           [key, version - retain])

                return db.commit()
    finally:
        db.rollback()
        db.close()

    raise Exception(f'STALE_PROPOSAL_SEQ {key}:{version} {seq}')


class RPCClient(httprpc.Client):
    def __init__(self, cacert, cert, servers):
        super().__init__(cacert, cert, servers)
        self.quorum = max(self.quorum, G.quorum)

    async def __call__(self, resource, octets=b''):
        res = await self.cluster(resource, octets)
        result = list()

        exceptions = list()
        for s, r in zip(self.conns.keys(), res):
            if isinstance(r, Exception):
                exceptions.append(f'\n-{s}\n{r}')
            else:
                result.append(r)

        if len(result) < self.quorum:
            raise Exception('\n'.join(exceptions))

        return result


class Client():
    def __init__(self, cacert, cert, servers):
        self.rpc = RPCClient(cacert, cert, servers)

    async def paxos_propose(self, db, key, version, retain=0, obj=b''):
        seq = int(time.time()*10**9)  # Current microsecond is a good enough
        url = f'paxos/db/{db}/key/{key}/version/{version}/seq/{seq}'

        if obj != b'':
            # value to be set should always be json serializable
            octets = gzip.compress(json.dumps(obj).encode())

        # Paxos PROMISE phase - block stale writers and finalize the proposal
        accepted_seq = 0
        for v in await self.rpc(f'{url}/retain/{retain}'):
            # CRUX of the paxos protocol - Find the most recent accepted value
            if v['accepted_seq'] > accepted_seq:
                accepted_seq, octets = v['accepted_seq'], v['value']

        # Paxos ACCEPT phase - propose the value found above
        # This may fail but we don't check the return value as we can't take
        # any corrective action. Entire process must be retried.
        await self.rpc(f'{url}/retain/{retain}', octets)

    async def get(self, db, key=None, version=None):
        url = f'read_server/db/{db}'
        version = int(version) if version is not None else None

        # Return the merged and deduplicated list of keys from all the nodes
        if key is None:
            keys = [k for i in await self.rpc(url) for j in i for k in j]
            return dict(db=db, keys=sorted(set(keys)))

        elif key is not None and version is None:
            # Verify if the value for a key-version has been finalized
            for i in range(self.rpc.quorum):
                res = await self.rpc(f'{url}/key/{key}')

                # version,value pair returned by all the nodes must be same
                if all([res[0] == v for v in res]):
                    if res[0] is None:
                        return dict(db=db, key=key, version=None)

                    return dict(
                        db=db, key=key, version=res[0][0],
                        value=json.loads(gzip.decompress(res[0][1]).decode()))

                # All the nodes do not agree on a version-value for this key
                # yet. Start a paxos round to build the consensus on the
                # highest version.
                version = max([v[0] for v in res if v and v[0] is not None])
                await self.paxos_propose(db, key, version, version)
        elif key is not None and version is not None:
            for i in range(self.rpc.quorum):
                res = await self.rpc(f'{url}/key/{key}/version/{version}')

                # value returned by all the nodes must be same
                if all([res[0] == v for v in res]):
                    if res[0] is None:
                        return dict(db=db, key=key, version=version)

                    return dict(
                        db=db, key=key, version=version,
                        value=json.loads(gzip.decompress(res[0][0]).decode()))

                # All the nodes do not agree on a value for this key-version
                # yet. Start a paxos round to build the consensus on this
                # key-verison.
                await self.paxos_propose(db, key, version, version)

    def hmac(self, x, y):
        return hmac.new(x.encode(), y.encode(), hashlib.sha256).hexdigest()

    async def put(self, db, secret, key, version, obj, retain=0):
        try:
            res = await self.get(db, db)
        except Exception:
            guid = str(uuid.uuid4())
            auth = dict(guid=guid, hmac=self.hmac(secret, guid))
            await self.paxos_propose(db, db, 0, 0, auth)
            res = await self.get(db, db)

        if db == key:
            guid = str(uuid.uuid4())
            obj = dict(guid=guid, hmac=self.hmac(obj, guid))

        if res['value']['hmac'] == self.hmac(secret, res['value']['guid']):
            # Run a paxos round to build consensus
            await self.paxos_propose(db, key, version, retain, obj)

            # Paxos guarantees that the value for the returned version is now
            # final and would not change under any condition.
            return await self.get(db, key, version)

        raise Exception('Authentication Failed')


async def get_server(ctx, db, key=None, version=None):
    return await Client(G.cert, G.cert, G.servers).get(db, key, version)


async def put_server(ctx, db, secret, key, version, obj, retain=0):
    return await Client(G.cert, G.cert, G.servers).put(
        db, secret, key, version, obj, retain)


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    P = argparse.ArgumentParser()
    P.add_argument('--cert', help='certificate path')
    P.add_argument('--port', type=int, help='port number for server')
    P.add_argument('--quorum', type=int, default=0, help='quorum override')
    P.add_argument('--servers', help='comma separated list of server ip:port')
    P.add_argument('--db', help='db for get/put')
    P.add_argument('--key', help='key for get/put')
    P.add_argument('--retain', type=int, help='retain this many old versions')
    P.add_argument('--version', type=int, help='version for put')
    G = P.parse_args()

    if G.port and G.cert and G.servers and G.db is None:
        # Start the server
        httprpc.run(G.port, dict(get=get_server, put=put_server,
                                 read_server=read_server, paxos=paxos_server),
                    cacert=G.cert, cert=G.cert)

    elif G.db and G.key and G.cert and G.servers and G.port is None:
        if G.retain is not None:
            # Write the value for this key, version
            # Remove older and retain only latest G.retain versions
            asyncio.run(Client(G.cert, G.cert, G.servers).paxos_propose(
                G.db, G.key, G.version, G.retain,
                json.loads(sys.stdin.read())))

        # Read the value
        print(json.dumps(asyncio.run(get_server({}, G.db, G.key, G.version)),
                         sort_keys=True, indent=4))
    else:
        P.print_help()
        exit(1)
