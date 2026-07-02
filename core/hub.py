import asyncio
import json
import time
import logging
import os
import ssl
from dataclasses import dataclass, field

import websockets
from websockets.server import WebSocketServerProtocol
from jsonschema import validate, ValidationError

# Structured Logging Setup
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "t": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "msg": record.getMessage(),
            "name": record.name
        }
        if hasattr(record, "extra_data"):
            log_entry.update(record.extra_data)
        return json.dumps(log_entry)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter(datefmt="%H:%M:%S"))
logging.basicConfig(level=logging.INFO, handlers=[handler])
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
    last_seen: float = field(default_factory=time.time)


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
                    api_key = msg.get("api_key")
                    if HUB_API_KEY and api_key != HUB_API_KEY:
                        log.warning(f"AUTH FAILED", extra={"extra_data": {"peer_id": msg.get("id")}})
                        await ws.send(json.dumps({"type": "error", "message": "unauthorized"}))
                        await ws.close()
                        return

                    peer_id = msg["id"]
                    self.peers[peer_id] = Peer(id=peer_id, ws=ws,
                                                capabilities=msg.get("capabilities", []),
                                                schemas=msg.get("schemas", {}),
                                                meta=msg.get("meta", {}))
                    authenticated = True
                    log.info(f"REGISTERED", extra={"extra_data": {"peer_id": peer_id, "caps": msg.get("capabilities")}})
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

                elif mtype == "heartbeat":
                    if peer_id in self.peers:
                        self.peers[peer_id].last_seen = time.time()

                elif mtype == "invoke":
                    target_id = msg["target"]
                    action = msg.get("action")
                    payload = msg.get("payload")

                    target_peer = self.peers.get(target_id)
                    if not target_peer:
                        await self._send(peer_id, {"type": "error", "req_id": msg["req_id"],
                                                     "message": f"target '{target_id}' not reachable"})
                        continue

                    schema = target_peer.schemas.get(action)
                    if schema:
                        try:
                            validate(instance=payload, schema=schema)
                        except ValidationError as e:
                            log.warning(f"VALIDATION FAILED", extra={"extra_data": {"src": peer_id, "dst": target_id, "action": action, "err": e.message}})
                            await self._send(peer_id, {
                                "type": "result", "req_id": msg["req_id"], "source": target_id,
                                "ok": False, "payload": f"Schema validation error: {e.message}"
                            })
                            continue

                    self._record("invoke", peer_id, target_id, {"action": action})
                    ok = await self._send(target_id, {
                        "type": "invoke", "req_id": msg["req_id"], "source": peer_id,
                        "action": action, "payload": payload,
                    })
                    log.info(f"INVOKE", extra={"extra_data": {"src": peer_id, "dst": target_id, "action": action, "ok": ok}})
                    if not ok:
                        await self._send(peer_id, {"type": "error", "req_id": msg["req_id"],
                                                     "message": f"target '{target_id}' not reachable"})

                elif mtype == "result":
                    target = msg["target"]
                    self._record("result", peer_id, target, {"ok": msg.get("ok")})
                    await self._send(target, {
                        "type": "result", "req_id": msg["req_id"], "source": peer_id,
                        "ok": msg.get("ok", True), "payload": msg.get("payload"),
                    })
                    log.info(f"RESULT", extra={"extra_data": {"src": peer_id, "dst": target, "ok": msg.get("ok", True)}})

                elif mtype == "subscribe":
                    topic = msg["topic"]
                    self.subscriptions.setdefault(topic, set()).add(peer_id)
                    log.info(f"SUBSCRIBE", extra={"extra_data": {"peer_id": peer_id, "topic": topic}})

                elif mtype == "publish":
                    topic = msg["topic"]
                    subs = self.subscriptions.get(topic, set())
                    for sub_id in subs:
                        if sub_id == peer_id:
                            continue
                        self._record("event", peer_id, sub_id, {"topic": topic})
                        await self._send(sub_id, {"type": "event", "topic": topic,
                                                    "source": peer_id, "payload": msg.get("payload")})
                    log.info(f"PUBLISH", extra={"extra_data": {"peer_id": peer_id, "topic": topic, "fanout": len(subs)}})

                elif mtype == "discovery":
                    await self._send(peer_id, {
                        "type": "discovery_result",
                        "req_id": msg.get("req_id"),
                        "peers": [{"id": p.id, "capabilities": p.capabilities, "schemas": p.schemas, "last_seen": p.last_seen}
                                  for p in self.peers.values()]
                    })

                else:
                    await self._send(peer_id or "unknown", {"type": "error", "message": f"unknown type {mtype}"})

        except websockets.ConnectionClosed:
            pass
        finally:
            if peer_id and peer_id in self.peers:
                del self.peers[peer_id]
                for subs in self.subscriptions.values():
                    subs.discard(peer_id)
                log.info(f"DISCONNECTED", extra={"extra_data": {"peer_id": peer_id}})
                for other in list(self.peers.values()):
                    await self._send(other.id, {"type": "peer_left", "id": peer_id})


container = ToolsContainer()


async def main(host="localhost", port=8765, certfile=None, keyfile=None):
    async def handler(ws):
        await container.handle(ws)

    ssl_context = None
    if certfile and keyfile:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(certfile=certfile, keyfile=keyfile)

    async def cleanup_stale_peers():
        while True:
            await asyncio.sleep(30)
            now = time.time()
            stale = [p_id for p_id, p in list(container.peers.items()) if now - p.last_seen > 90]
            for p_id in stale:
                log.warning(f"STALE PEER REMOVED", extra={"extra_data": {"peer_id": p_id}})
                peer = container.peers.pop(p_id)
                try:
                    await peer.ws.close()
                except:
                    pass

    async with websockets.serve(handler, host, port, ssl=ssl_context):
        scheme = "wss" if ssl_context else "ws"
        log.info(f"HUB STARTED", extra={"extra_data": {"uri": f"{scheme}://{host}:{port}"}})
        cleanup_task = asyncio.create_task(cleanup_stale_peers())
        try:
            await asyncio.Future()  # run forever
        finally:
            cleanup_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
