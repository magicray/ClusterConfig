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


def get_db(db):
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


# PROMISE - Block stale writers and return the most recent accepted value.
# Client will propose the most recent across servers in the accept phase
async def paxos_promise(ctx, db, key, version, proposal_seq):
    version = int(version)
    proposal_seq = int(proposal_seq)

    db = get_db(db)
    try:
        db.execute('insert or ignore into paxos values(?,?,0,0,null)',
                   [key, version])

        promised_seq, accepted_seq, value = db.execute(
            '''select promised_seq, accepted_seq, value
               from paxos where key=? and version=?
            ''', [key, version]).fetchone()

        if proposal_seq <= promised_seq:
            raise Exception(f'OLD_PROMISE_SEQ {key}:{version} {proposal_seq}')

        db.execute('update paxos set promised_seq=? where key=? and version=?',
                   [proposal_seq, key, version])
        db.commit()

        return dict(accepted_seq=accepted_seq, value=value)
    finally:
        db.rollback()
        db.close()


# ACCEPT - Client has sent the most recent value from the promise phase.
async def paxos_accept(ctx, db, key, version, proposal_seq, octets):
    version = int(version)
    proposal_seq = int(proposal_seq)

    if not octets:
        raise Exception('NULL_VALUE')

    db = get_db(db)
    try:
        db.execute('insert or ignore into paxos values(?,?,0,0,null)',
                   [key, version])

        promised_seq = db.execute(
            'select promised_seq from paxos where key=? and version=?',
            [key, version]).fetchone()[0]

        if proposal_seq < promised_seq:
            raise Exception(f'OLD_ACCEPT_SEQ {key}:{version} {proposal_seq}')

        db.execute('delete from paxos where key=? and version<?',
                   [key, version])
        db.execute('''update paxos set promised_seq=?, accepted_seq=?, value=?
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


# Return the row with the highest version for this key with accepted value
async def read(ctx, db, key):
    db = get_db(db)
    try:
        version, accepted_seq, value = db.execute(
            '''select version, accepted_seq, value from paxos
               where key=? and accepted_seq > 0
               order by version desc limit 1
            ''', [key]).fetchone()

        return dict(version=version, accepted_seq=accepted_seq, value=value)
    finally:
        db.rollback()
        db.close()


# Return the keys with latest accepted version
async def key_list(ctx, db):
    db = get_db(db)
    try:
        rows = db.execute('''select key, version from paxos
                             where accepted_seq > 0
                          ''')

        return rows.fetchall()
    finally:
        db.rollback()
        db.close()


# GET Client
async def get(ctx, db, key):
    for i in range(G.client.quorum):
        res = await G.client.filtered(f'read/db/{db}/key/{key}')
        if G.client.quorum > len(res):
            raise Exception('NO_READ_QUORUM')

        vlist = [v for v in res.values()]
        if all([vlist[0] == v for v in vlist]):
            return dict(key=key, version=vlist[0]['version'],
                        value=json.loads(vlist[0]['value'].decode()))

        await put(ctx, db, key, max([v['version'] for v in vlist]), '')


# PUT Client
async def put(ctx, db, secret, key, version, obj):
    if type(secret) is not int:
        result = await get(ctx, db, '-')
        key_hmac = hmac.new(secret.encode(), result['value']['salt'].encode(),
                            digestmod=hashlib.sha256).hexdigest()
        if key_hmac != result['value']['hmac']:
            raise Exception('Authentication Failed')

    seq = int(time.strftime('%Y%m%d%H%M%S'))
    url = f'db/{db}/key/{key}/version/{version}/proposal_seq/{seq}'

    value = json.dumps(obj).encode()

    # Paxos PROMISE phase - block stale writers
    res = await G.client.filtered(f'/promise/{url}')
    if G.client.quorum > len(res):
        raise Exception('NO_PROMISE_QUORUM')

    # CRUX of the paxos protocol - Find the most recent accepted value
    accepted_seq = 0
    for v in res.values():
        if v['accepted_seq'] > accepted_seq:
            accepted_seq, value = v['accepted_seq'], v['value']

    # Paxos ACCEPT phase - propose the value found above
    res = await G.client.filtered(f'/accept/{url}', value)
    if G.client.quorum > len(res):
        raise Exception('NO_ACCEPT_QUORUM')

    if not all([1 == v['count'] for v in res.values()]):
        raise Exception('ACCEPT_FAILED')

    return dict(key=key, version=version, value=json.loads(value.decode()),
                status='CONFLICT' if accepted_seq > 0 else 'OK')


# LIST Client
async def keys(ctx, db):
    for i in range(G.client.quorum):
        res = await G.client.filtered(f'key_list/db/{db}')
        if G.client.quorum > len(res):
            raise Exception('NO_READ_QUORUM')

        result = dict()
        for values in res.values():
            for key, version in values:
                result[key] = version

        return result


# Initialize the db and generate api key
async def init(ctx, db, secret=None):
    version = 1

    if secret is not None:
        result = await get(ctx, db, '-')
        key_hmac = hmac.new(secret.encode(), result['value']['salt'].encode(),
                            digestmod=hashlib.sha256).hexdigest()
        if key_hmac != result['value']['hmac']:
            raise Exception('Authentication Failed')

        version = result['version'] + 1

    salt = str(uuid.uuid4())
    secret = str(uuid.uuid4())
    key_hmac = hmac.new(secret.encode(), salt.encode(),
                        digestmod=hashlib.sha256).hexdigest()

    res = await put(ctx, db, 0, '-', version, dict(salt=salt, hmac=key_hmac))
    res.pop('value', None)

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

    G.client = RPCClient(G.cert, G.cert, G.servers)
    httprpc.run(G.port, dict(init=init, put=put, get=get,
                             keys=keys, key_list=key_list, read=read,
                             promise=paxos_promise, accept=paxos_accept),
                cacert=G.cert, cert=G.cert)
