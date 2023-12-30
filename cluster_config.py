import os
import time
import json
import uuid
import hmac
import sqlite3
import logging
import httprpc
import hashlib
import argparse
from logging import critical as log


def connect_db(db):
    os.makedirs('cluster_config', exist_ok=True)

    db = sqlite3.connect(os.path.join('cluster_config', db + '.sqlite3'))
    db.execute('''create table if not exists paxos(
                      key          text,
                      version      int,
                      promised_seq int,
                      accepted_seq int,
                      value        blob,
                      primary key(key, version)
                  )''')

    return db


async def fetch(ctx, db, key=None):
    db = connect_db(db)
    try:
        if key is None:
            return db.execute('''select key, version from paxos
                                 where accepted_seq > 0
                                 order by key, version
                              ''').fetchall()
        else:
            row = db.execute('''select version, accepted_seq, value from paxos
                                where key=? and accepted_seq > 0
                                order by version desc limit 1
                             ''', [key]).fetchone()
            return row if row else [0, 0, None]
    finally:
        db.close()


async def paxos_server(ctx, db, key, version, proposal_seq, octets=None):
    version = int(version)
    proposal_seq = int(proposal_seq)

    if version < 1:
        raise Exception(f'INVALID_VERSION - {version}')

    db = connect_db(db)
    try:
        db.execute('insert or ignore into paxos values(?,?,0,0,null)',
                   [key, version])

        current_version = db.execute('''select max(version) from paxos
                                        where key=? and accepted_seq > 0
                                     ''', [key]).fetchone()[0]

        if current_version is not None and version < current_version:
            raise Exception(f'STALE_VERSION - {version}')

        if octets is None:
            # Paxos PROMISE - Block stale writers and return the most recent
            # accepted value. Client will propose the most recent across
            # servers in the accept phase
            promised_seq, accepted_seq, value = db.execute(
                '''select promised_seq, accepted_seq, value
                   from paxos where key=? and version=?
                ''', [key, version]).fetchone()

            if proposal_seq <= promised_seq:
                raise Exception(f'PROMISE_SEQ {key}:{version} {proposal_seq}')

            db.execute('''update paxos set promised_seq=?
                          where key=? and version=?
                       ''', [proposal_seq, key, version])
            db.commit()

            return dict(accepted_seq=accepted_seq, value=value)
        else:
            # Paxos ACCEPT - Client has sent the most recent value from the
            # promise phase.
            promised_seq = db.execute(
                'select promised_seq from paxos where key=? and version=?',
                [key, version]).fetchone()[0]

            if proposal_seq < promised_seq:
                raise Exception(f'ACCEPT_SEQ {key}:{version} {proposal_seq}')

            db.execute('delete from paxos where key=? and version<?',
                       [key, version])

            db.execute('''update paxos
                          set promised_seq=?, accepted_seq=?, value=?
                          where key=? and version=?
                       ''', [proposal_seq, proposal_seq, octets, key, version])
            db.commit()

            count = db.execute('''select count(*) from paxos
                                  where key=? and version=?
                               ''', [key, version]).fetchone()[0]

            return dict(count=count)
    finally:
        db.rollback()
        db.close()


# PROPOSE - Drives the paxos protocol
async def paxos_client(client, db, key, version, obj):
    seq = int(time.strftime('%Y%m%d%H%M%S'))
    url = f'paxos/db/{db}/key/{key}/version/{version}/proposal_seq/{seq}'

    value = json.dumps(obj).encode()

    # Paxos PROMISE phase - block stale writers
    res = await client.filtered(url)
    if client.quorum > len(res):
        raise Exception('NO_PROMISE_QUORUM')

    # CRUX of the paxos protocol - Find the most recent accepted value
    accepted_seq = 0
    for v in res.values():
        if v['accepted_seq'] > accepted_seq:
            accepted_seq, value = v['accepted_seq'], v['value']

    # Paxos ACCEPT phase - propose the value found above
    res = await client.filtered(url, value)
    if client.quorum > len(res):
        raise Exception('NO_ACCEPT_QUORUM')

    if not all([1 == v['count'] for v in res.values()]):
        raise Exception('ACCEPT_FAILED')

    return dict(key=key, version=version, value=json.loads(value.decode()),
                db=db, status='CONFLICT' if accepted_seq > 0 else 'OK')


async def get(ctx, db, key=None):
    client = ctx.get('client', RPCClient(G.cert, G.cert, G.servers))

    if key is None:
        res = await client.filtered(f'fetch/db/{db}')
        if client.quorum > len(res):
            raise Exception('NO_READ_QUORUM')

        result = dict()
        for values in res.values():
            for key, version in values:
                result[key] = version

        return result
    else:
        for i in range(client.quorum):
            res = await client.filtered(f'fetch/db/{db}/key/{key}')
            if client.quorum > len(res):
                raise Exception('NO_READ_QUORUM')

            vlist = [v for v in res.values()]
            if all([vlist[0] == v for v in vlist]):
                result = dict(db=db, key=key, version=vlist[0][0])

                if vlist[0][0] > 0:
                    result['value'] = json.loads(vlist[0][2].decode())

                return result

            await paxos_client(db, key, max([v[0] for v in vlist]), '')


def get_hmac(secret, salt):
    return hmac.new(secret.encode(), salt.encode(), hashlib.sha256).hexdigest()


async def put(ctx, db, secret, key, version, obj):
    ctx['client'] = RPCClient(G.cert, G.cert, G.servers)

    result = await get(ctx, db, '#')
    if result['value']['hmac'] != get_hmac(secret, result['value']['salt']):
        raise Exception('Authentication Failed')

    return await paxos_client(ctx['client'], db, key, version, obj)


# Initialize the db and generate api key
async def init(ctx, db, secret=None):
    ctx['client'] = RPCClient(G.cert, G.cert, G.servers)

    version = 1
    if secret is not None:
        res = await get(ctx, db, '#')
        if res['value']['hmac'] != get_hmac(secret, res['value']['salt']):
            raise Exception('Authentication Failed')

        version = res['version'] + 1

    salt = str(uuid.uuid4())
    secret = str(uuid.uuid4())

    res = await paxos_client(ctx['client'], db, '#', version,
                             dict(salt=salt, hmac=get_hmac(secret, salt)))
    if 'OK' == res['status']:
        res['secret'] = secret

    return res


class RPCClient(httprpc.Client):
    def __init__(self, cacert, cert, servers):
        super().__init__(cacert, cert, servers)

    async def filtered(self, resource, octets=b''):
        res = await self.cluster(resource, octets)
        result = dict()

        for s, r in zip(self.conns.keys(), res):
            if isinstance(r, Exception):
                log(f'{s} {type(r)} {r}')
            else:
                result[s] = r

        return result


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--port', help='port number for server')
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G = G.parse_args()

    httprpc.run(G.port, dict(init=init, get=get, put=put,
                             fetch=fetch, paxos=paxos_server),
                cacert=G.cert, cert=G.cert)
