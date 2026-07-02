import asyncio
import json
import time
import logging
import os
from dataclasses import dataclass, field

import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("hub")

# Simple API Key security
HUB_API_KEY = os.environ.get("HUB_API_KEY", "secret-key")

@dataclass
class Peer:
    id: str
    ws: WebSocketServerProtocol
    capabilities: list = field(default_factory=list)
    schemas: dict = field(default_factory=dict)
    meta: dict = field(default_factory=dict)


class ToolsContainer:
    def __init__(self):
        self.peers: dict[str, Peer] = {}
        self.subscriptions: dict[str, set[str]] = {}  # topic -> set of peer ids
        self.relations_log: list[dict] = []            # empirical edge log

    def _record(self, kind: str, src: str, dst: str, extra: dict | None = None):
        self.relations_log.append({
            "t": round(time.time(), 3), "kind": kind, "src": src, "dst": dst,
            **(extra or {}),
        })

    async def _send(self, peer_id: str, msg: dict):
        peer = self.peers.get(peer_id)
        if peer is None:
            return False
        try:
            await peer.ws.send(json.dumps(msg))
            return True
        except websockets.ConnectionClosed:
            return False

    async def handle(self, ws: WebSocketServerProtocol):
        peer_id = None
        authenticated = False
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    await ws.send(json.dumps({"type": "error", "message": "bad json"}))
                    continue

                mtype = msg.get("type")

                if mtype == "register":
                    # Security check
                    api_key = msg.get("api_key")
                    if HUB_API_KEY and api_key != HUB_API_KEY:
                        log.warning(f"AUTH FAILED for tool {msg.get('id')}")
                        await ws.send(json.dumps({"type": "error", "message": "unauthorized"}))
                        await ws.close()
                        return

                    peer_id = msg["id"]
                    self.peers[peer_id] = Peer(id=peer_id, ws=ws,
                                                capabilities=msg.get("capabilities", []),
                                                schemas=msg.get("schemas", {}),
                                                meta=msg.get("meta", {}))
                    authenticated = True
                    log.info(f"REGISTER {peer_id} caps={msg.get('capabilities', [])}")
                    await self._send(peer_id, {
                        "type": "registered", "id": peer_id,
                        "peers": [{"id": p.id, "capabilities": p.capabilities, "schemas": p.schemas}
                                  for p in self.peers.values() if p.id != peer_id],
                    })
                    for other in list(self.peers.values()):
                        if other.id != peer_id:
                            await self._send(other.id, {
                                "type": "peer_joined", "id": peer_id,
                                "capabilities": msg.get("capabilities", []),
                                "schemas": msg.get("schemas", {})
                            })

                elif not authenticated:
                    await ws.send(json.dumps({"type": "error", "message": "not registered"}))
                    continue

                elif mtype == "invoke":
                    target = msg["target"]
                    self._record("invoke", peer_id, target, {"action": msg.get("action")})
                    ok = await self._send(target, {
                        "type": "invoke", "req_id": msg["req_id"], "source": peer_id,
                        "action": msg.get("action"), "payload": msg.get("payload"),
                    })
                    log.info(f"INVOKE  {peer_id} -> {target}  action={msg.get('action')}  delivered={ok}")
                    if not ok:
                        await self._send(peer_id, {"type": "error", "req_id": msg["req_id"],
                                                     "message": f"target '{target}' not reachable"})

                elif mtype == "result":
                    target = msg["target"]
                    self._record("result", peer_id, target, {"ok": msg.get("ok")})
                    await self._send(target, {
                        "type": "result", "req_id": msg["req_id"], "source": peer_id,
                        "ok": msg.get("ok", True), "payload": msg.get("payload"),
                    })
                    log.info(f"RESULT  {peer_id} -> {target}  ok={msg.get('ok', True)}")

                elif mtype == "subscribe":
                    topic = msg["topic"]
                    self.subscriptions.setdefault(topic, set()).add(peer_id)
                    log.info(f"SUBSCRIBE {peer_id} -> topic:{topic}")

                elif mtype == "publish":
                    topic = msg["topic"]
                    subs = self.subscriptions.get(topic, set())
                    for sub_id in subs:
                        if sub_id == peer_id:
                            continue
                        self._record("event", peer_id, sub_id, {"topic": topic})
                        await self._send(sub_id, {"type": "event", "topic": topic,
                                                    "source": peer_id, "payload": msg.get("payload")})
                    log.info(f"PUBLISH {peer_id} -> topic:{topic}  fanout={len(subs)}")

                else:
                    await self._send(peer_id or "unknown", {"type": "error", "message": f"unknown type {mtype}"})

        except websockets.ConnectionClosed:
            pass
        finally:
            if peer_id and peer_id in self.peers:
                del self.peers[peer_id]
                for subs in self.subscriptions.values():
                    subs.discard(peer_id)
                log.info(f"DISCONNECT {peer_id}")
                for other in list(self.peers.values()):
                    await self._send(other.id, {"type": "peer_left", "id": peer_id})


container = ToolsContainer()


async def main(host="localhost", port=8765):
    async def handler(ws):
        await container.handle(ws)

    async with websockets.serve(handler, host, port):
        log.info(f"tools container listening on ws://{host}:{port}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
