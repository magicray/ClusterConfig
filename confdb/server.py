import os
import pickle
import sqlite3
import logging
import httprpc
import argparse


def init_db(db, key, version):
    db.execute('''create table if not exists kv(
                      key          text,
                      version      int,
                      promised_seq int,
                      accepted_seq int,
                      value        blob,
                      primary key(key, version)
                  )''')

    db.execute('insert or ignore into kv values(?,?,0,0,null)', [key, version])


# PROMISE - Block stale writers and return the most recent accepted value.
# Client will propose the most recent across servers in the accept phase
async def paxos_promise(ctx, key, version, proposal_seq):
    version = int(version)
    proposal_seq = int(proposal_seq)

    db = sqlite3.connect(os.path.join('confdb', ctx['subject'] + '.sqlite3'))
    try:
        init_db(db, key, version)

        promised_seq, accepted_seq, value = db.execute(
            '''select promised_seq, accepted_seq, value
               from kv where key=? and version=?
            ''', [key, version]).fetchone()

        if proposal_seq <= promised_seq:
            raise Exception(f'OLD_PROMISE_SEQ {key}:{version} {proposal_seq}')

        db.execute('update kv set promised_seq=? where key=? and version=?',
                   [proposal_seq, key, version])
        db.commit()

        return pickle.dumps(dict(accepted_seq=accepted_seq, value=value))
    finally:
        db.rollback()
        db.close()


# ACCEPT - Client has sent the most recent value from the promise phase.
async def paxos_accept(ctx, key, version, proposal_seq, octets):
    version = int(version)
    proposal_seq = int(proposal_seq)

    if not octets:
        raise Exception('NULL_VALUE')

    db = sqlite3.connect(os.path.join('confdb', ctx['subject'] + '.sqlite3'))
    try:
        init_db(db, key, version)

        promised_seq = db.execute(
            'select promised_seq from kv where key=? and version=?',
            [key, version]).fetchone()[0]

        if proposal_seq < promised_seq:
            raise Exception(f'OLD_ACCEPT_SEQ {key}:{version} {proposal_seq}')

        db.execute('delete from kv where key=? and version<?', [key, version])
        db.execute('''update kv set promised_seq=?, accepted_seq=?, value=?
                      where key=? and version=?''',
                   [proposal_seq, proposal_seq, octets, key, version])
        db.commit()

        count = db.execute('select count(*) from kv where key=? and version=?',
                           [key, version]).fetchone()[0]

        return pickle.dumps(dict(count=count))
    finally:
        db.rollback()
        db.close()


# Return the row with the highest version for this key with accepted value
async def read(ctx, key):
    db = sqlite3.connect(os.path.join('confdb', ctx['subject'] + '.sqlite3'))
    try:
        version, accepted_seq, value = db.execute(
            '''select version, accepted_seq, value
               from kv where key=? and accepted_seq > 0
               order by version desc limit 1''',
            [key]).fetchone()

        return pickle.dumps(dict(version=version, accepted_seq=accepted_seq,
                                 value=value))
    finally:
        db.rollback()
        db.close()


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--port', help='port number for server')
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G = G.parse_args()

    os.makedirs('confdb', exist_ok=True)
    httprpc.run(G.cacert, G.cert, G.port, dict(
        read=read, promise=paxos_promise, accept=paxos_accept))
