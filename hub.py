"""
hub.py — the Tools Container.

A single WebSocket process that tools connect to as peers. It does three things:
  1. REGISTRY   — tracks which tools are alive and what they can do (capabilities).
  2. RELAY      — routes point-to-point invoke/result messages between tools by id,
                  correlated with req_id so concurrent calls don't collide.
  3. PUB/SUB    — lets tools broadcast events to topic subscribers without knowing
                  who's listening (loose coupling for things like "solver_progress").

Every routed message is also appended to `relations_log` — this is the empirical
record of *actual* tool-to-tool relations (who talked to whom, how often, about what),
as opposed to a relations diagram someone designed before running anything.

Protocol (JSON frames over one WS connection per tool):
  -> register    {type, id, capabilities: [str], meta?: {}}
  <- registered  {type, id, peers: [{id, capabilities}]}
  <- peer_joined {type, id, capabilities}
  <- peer_left   {type, id}
  -> invoke      {type, req_id, target, action, payload}
  <- invoke      {type, req_id, source, action, payload}      (delivered to target)
  -> result      {type, req_id, target, ok, payload}
  <- result      {type, req_id, source, ok, payload}          (delivered back to caller)
  -> subscribe   {type, topic}
  -> publish     {type, topic, payload}
  <- event       {type, topic, source, payload}
  -> error / <- error  {type, message}
"""
import asyncio
import json
import time
import logging
from dataclasses import dataclass, field

import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("hub")


@dataclass
class Peer:
    id: str
    ws: WebSocketServerProtocol
    capabilities: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)


class ToolsContainer:
    def __init__(self):
        self.peers: dict[str, Peer] = {}
        self.subscriptions: dict[str, set[str]] = {}  # topic -> set of peer ids
        self.relations_log: list[dict] = []            # empirical edge log
        self.pending: dict[str, str] = {}               # req_id -> caller id (for error handling)

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
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    await ws.send(json.dumps({"type": "error", "message": "bad json"}))
                    continue

                mtype = msg.get("type")

                if mtype == "register":
                    peer_id = msg["id"]
                    self.peers[peer_id] = Peer(id=peer_id, ws=ws,
                                                capabilities=msg.get("capabilities", []),
                                                meta=msg.get("meta", {}))
                    log.info(f"REGISTER {peer_id} caps={msg.get('capabilities', [])}")
                    await self._send(peer_id, {
                        "type": "registered", "id": peer_id,
                        "peers": [{"id": p.id, "capabilities": p.capabilities}
                                  for p in self.peers.values() if p.id != peer_id],
                    })
                    for other in list(self.peers.values()):
                        if other.id != peer_id:
                            await self._send(other.id, {"type": "peer_joined", "id": peer_id,
                                                          "capabilities": msg.get("capabilities", [])})

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
