import time
import pickle
import asyncio
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

        return 'CONFLICT' if accepted_seq > 0 else 'OK'

    async def get(self, key):
        for i in range(self.quorum):
            res = await self.client.filtered(f'/read/key/{key}')
            if self.quorum > len(res):
                await asyncio.sleep(1)
                continue

            vlist = [pickle.loads(v) for v in res.values()]
            if all([vlist[0] == v for v in vlist]):
                return dict(value=vlist[0]['value'],
                            version=vlist[0]['version'])

            for v in vlist:
                new = v['version'], v['accepted_seq']
                old = vlist[0]['version'], vlist[0]['accepted_seq']

                if new > old:
                    vlist[0] = v

            await self.put(key, vlist[0]['version'], vlist[0]['value'])
