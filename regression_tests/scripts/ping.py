import os
import sys
import json
import asyncio
import uuid
from quantnet_mq.rpcclient import RPCClient
from quantnet_mq.msgserver import MsgServer
from quantnet_mq.schema.models import Schema

ret = None


class MyPingPonger():
    def __init__(self, destinations=list(), iters=5):
        self._dests = destinations
        self._iters = iters
        self._pending = len(self._dests)
        self._token = str(uuid.uuid4())

    async def start_pingpong(self, client):
        msg = {"type": "ping", "destinations": self._dests,
               "iterations": self._iters,
               "token": self._token}
        ret = await client.call("pingpong", msg, timeout=20.0)
        ret = json.loads(ret)
        return ret

    async def handle_pong(self, msg):
        from quantnet_mq.schema.models import pingpong
        res = pingpong.pingPongRecord(**json.loads(msg))
        print(f"--- {res.agent} ping statistics ---")
        print(f"{res.iterations} requests made, {res.successes} received, time {(res.end_ts-res.start_ts)*1e3:.0f}ms")
        if res.successes:
            rtt_min = float(res.rtt_min)
            rtt_avg = float(res.rtt_avg)
            rtt_max = float(res.rtt_max)
            rtt_mdev = float(res.rtt_mdev)
            print(f"rtt min/avg/max/mdev {rtt_min:.3f}/{rtt_avg:.3f}/{rtt_max:.3f}/{rtt_mdev:.3f} ms")
            try:
                msg = eval(str(res.result))
                print(f"message: {msg.get('message')}")
            except Exception:
                pass
        self._pending -= 1

    async def main(self):
        # Setup RPC client with our PingPong schema
        Schema.load_schema("./regression_tests/conf/schema/pingpong.yaml", ns="pingpong")
        client = RPCClient(None, host=os.getenv("HOST", "localhost"))
        client.set_handler("pingpong", None, "quantnet_mq.schema.models.pingpong.pingPongRequest")
        await client.start()

        # Subscribe to pong topic
        mclient = MsgServer(host=os.getenv("HOST", "localhost"))
        mclient.subscribe(f"pong-{self._token}", self.handle_pong)
        await mclient.start()

        # Begin pingpong request
        res = await self.start_pingpong(client)

        # Wait for pong responses (as received at controller)
        while (self._pending > 0):
            await asyncio.sleep(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        dests = [d for d in sys.argv[1:]]
    else:
        dests = ["LBNL-SWITCH", "UCB-SWITCH", "UCB-Q", "LBNL-Q"]
    asyncio.run(MyPingPonger(dests, iters=5).main())
