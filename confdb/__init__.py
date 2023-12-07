import time
import json
import pickle
import httprpc


class RPCClient(httprpc.Client):
    def __init__(self, cacert, cert, servers):
        super().__init__(cacert, cert, servers)

    async def filtered(self, resource, octets=b''):
        res = await self.cluster(resource, octets)
        result = dict()

        for s, r in zip(self.conns.keys(), res):
            if r and type(r) is bytes:
                result[s] = r

        return result


class Client():
    def __init__(self, cacert, cert, servers):
        self.client = RPCClient(cacert, cert, servers)
        self.quorum = self.client.quorum

    # PAXOS Proposer
    async def put(self, key, version, value):
        seq = int(time.strftime('%Y%m%d%H%M%S'))
        url = f'key/{key}/version/{version}/proposal_seq/{seq}'

        if type(value) is not bytes:
            value = json.dumps(value, sort_keys=True, indent=4).encode()

        # Paxos PROMISE phase - block stale writers
        res = await self.client.filtered(f'/promise/{url}')
        if self.quorum > len(res):
            raise Exception('NO_PROMISE_QUORUM')

        # CRUX of the paxos protocol - Find the most recent accepted value
        accepted_seq = 0
        for v in res.values():
            v = pickle.loads(v)
            if v['accepted_seq'] > accepted_seq:
                accepted_seq, value = v['accepted_seq'], v['value']

        # Paxos ACCEPT phase - propose the value found above
        res = await self.client.filtered(f'/accept/{url}', value)
        if self.quorum > len(res):
            raise Exception('NO_ACCEPT_QUORUM')

        if not all([1 == pickle.loads(v)['count'] for v in res.values()]):
            raise Exception('ACCEPT_FAILED')

        return dict(key=key, version=version, value=json.loads(value.decode()),
                    status='CONFLICT' if accepted_seq > 0 else 'OK')

    async def get(self, key):
        for i in range(self.quorum):
            res = await self.client.filtered(f'/read/key/{key}')
            if self.quorum > len(res):
                raise Exception('NO_READ_QUORUM')

            vlist = [pickle.loads(v) for v in res.values()]
            if all([vlist[0] == v for v in vlist]):
                return dict(key=key, version=vlist[0]['version'],
                            value=json.loads(vlist[0]['value'].decode()))

            await self.put(key, max([v['version'] for v in vlist]), b'')
