import asyncio
import json
import uuid
import logging
import os

import websockets

log = logging.getLogger("tool_client")

HUB_API_KEY = os.environ.get("HUB_API_KEY", "secret-key")

class ToolClient:
    def __init__(self, tool_id: str, capabilities: list[str], schemas: dict = None, uri: str = "ws://localhost:8765"):
        self.id = tool_id
        self.capabilities = capabilities
        self.schemas = schemas or {}
        self.uri = uri
        self.ws = None
        self.peers: dict[str, dict] = {}
        self._invoke_handlers: dict[str, callable] = {}
        self._event_handlers: dict[str, callable] = {}
        self._pending: dict[str, asyncio.Future] = {}

    def on_invoke(self, action: str, fn):
        """fn(payload: dict) -> dict, sync or async"""
        self._invoke_handlers[action] = fn

    def on_event(self, topic: str, fn):
        self._event_handlers[topic] = fn

    async def connect(self, api_key: str = HUB_API_KEY):
        self.ws = await websockets.connect(self.uri)
        await self.ws.send(json.dumps({
            "type": "register",
            "id": self.id,
            "capabilities": self.capabilities,
            "schemas": self.schemas,
            "api_key": api_key
        }))
        raw = await self.ws.recv()
        first = json.loads(raw)
        if first["type"] == "error":
            raise RuntimeError(f"Connection failed: {first.get('message')}")
        assert first["type"] == "registered"
        for p in first["peers"]:
            self.peers[p["id"]] = {"capabilities": p["capabilities"], "schemas": p.get("schemas", {})}
        self._recv_task = asyncio.create_task(self._recv_loop())
        return self

    async def _recv_loop(self):
        try:
            async for raw in self.ws:
                await self._handle(json.loads(raw))
        except websockets.ConnectionClosed:
            pass

    async def subscribe(self, topic: str):
        await self.ws.send(json.dumps({"type": "subscribe", "topic": topic}))

    async def publish(self, topic: str, payload: dict):
        await self.ws.send(json.dumps({"type": "publish", "topic": topic, "payload": payload}))

    async def invoke(self, target: str, action: str, payload: dict, timeout: float = 10.0):
        req_id = str(uuid.uuid4())
        fut = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut
        await self.ws.send(json.dumps({"type": "invoke", "req_id": req_id, "target": target,
                                        "action": action, "payload": payload}))
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending.pop(req_id, None)

    async def _handle(self, msg: dict):
        mtype = msg["type"]
        if mtype == "peer_joined":
            self.peers[msg["id"]] = {"capabilities": msg["capabilities"], "schemas": msg.get("schemas", {})}
        elif mtype == "peer_left":
            self.peers.pop(msg["id"], None)
        elif mtype == "invoke":
            action, payload = msg["action"], msg["payload"]
            fn = self._invoke_handlers.get(action)
            ok, result = True, None
            if fn is None:
                ok, result = False, f"no handler for action '{action}'"
            else:
                try:
                    result = await fn(payload) if asyncio.iscoroutinefunction(fn) else fn(payload)
                except Exception as e:
                    ok, result = False, str(e)
            await self.ws.send(json.dumps({"type": "result", "req_id": msg["req_id"],
                                            "target": msg["source"], "ok": ok, "payload": result}))
        elif mtype == "result":
            fut = self._pending.get(msg["req_id"])
            if fut and not fut.done():
                if msg.get("ok", True):
                    fut.set_result(msg["payload"])
                else:
                    fut.set_exception(RuntimeError(str(msg["payload"])))
        elif mtype == "event":
            fn = self._event_handlers.get(msg["topic"])
            if fn:
                await fn(msg["payload"]) if asyncio.iscoroutinefunction(fn) else fn(msg["payload"])
        elif mtype == "error":
            log.warning(f"[{self.id}] error: {msg.get('message')}")

    async def run(self):
        await self._recv_task
