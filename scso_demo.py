import asyncio
import json
import logging
import time
from core.hub import main as run_hub, container
from core.tool_client import ToolClient
from core.scso_engine import SCSOEngine

logging.getLogger("tool_client").setLevel(logging.WARNING)
# logging.getLogger("hub").setLevel(logging.WARNING)

# =====================================================================
# 1. SPECIALIZED REMOTE SKILL PEERS
# =====================================================================

async def image_skill_tool():
    client = ToolClient("image_skill", capabilities=["classify", "get_corpus"])
    await client.connect()

    async def get_corpus(payload):
        return {
            "corpus": [
                "image classification pixels picture visual camera photograph photo",
                "object detection segment images deep learning convolutional network cnn"
            ],
            "initial_cost": 0.05
        }

    async def classify(payload):
        await asyncio.sleep(0.04)  # Simulate processing latency
        return {"result": f"Classified image: {payload.get('image_name')}", "status": "success"}

    client.on_invoke("get_corpus", get_corpus)
    client.on_invoke("classify", classify)
    await client.run()

async def code_skill_tool():
    client = ToolClient("code_skill", capabilities=["codegen", "get_corpus"])
    await client.connect()

    async def get_corpus(payload):
        return {
            "corpus": [
                "code generation python function algorithm programming",
                "compiler syntax parsing script developer automated software coding"
            ],
            "initial_cost": 0.08
        }

    async def codegen(payload):
        await asyncio.sleep(0.06)
        return {"result": f"Generated function: {payload.get('func_name')}", "status": "success"}

    client.on_invoke("get_corpus", get_corpus)
    client.on_invoke("codegen", codegen)
    await client.run()

async def sql_skill_tool():
    client = ToolClient("sql_skill", capabilities=["sql_execute", "get_corpus"])
    await client.connect()

    async def get_corpus(payload):
        return {
            "corpus": [
                "sql query database select insert table schema",
                "relational join query plan execution postgres db administration index"
            ],
            "initial_cost": 0.03
        }

    async def sql_execute(payload):
        await asyncio.sleep(0.02)
        return {"result": f"Executed query: {payload.get('query')}", "status": "success"}

    client.on_invoke("get_corpus", get_corpus)
    client.on_invoke("sql_execute", sql_execute)
    await client.run()


# =====================================================================
# 2. THE SCSO ROUTER GATEWAY
# =====================================================================

def make_remote_skill_class(gateway_client, peer_id, action):
    """
    Creates a dynamic skill proxy class that matches SCSOEngine's instantiation
    design, routing calls remotely through the WebSocket hub.
    """
    class RemoteSkillProxy:
        def __init__(self):
            self.gateway_client = gateway_client
            self.peer_id = peer_id
            self.action = action

        async def execute(self, payload: dict) -> dict:
            return await self.gateway_client.invoke(self.peer_id, self.action, payload)

    return RemoteSkillProxy

async def scso_gateway_tool(done_event: asyncio.Event):
    client = ToolClient("scso_gateway", capabilities=["route_request"])
    await client.connect()

    # Give peer clients a brief moment to join the registry
    await asyncio.sleep(0.5)

    # Configure the SCSO Engine with an LRU cache size of 2
    engine = SCSOEngine(max_instances=2, initial_threshold=0.25)

    print(f"[Gateway] Discovered skill peers on hub: {client.peers}")

    cap_to_action = {
        "classify": "classify",
        "codegen": "codegen",
        "sql_execute": "sql_execute"
    }

    # Automatically query other connected peers for their corpora and register them
    for peer_id, info in list(client.peers.items()):
        capabilities = info.get("capabilities", [])
        if "get_corpus" in capabilities:
            info_corpus = await client.invoke(peer_id, "get_corpus", {})
            corpus = info_corpus["corpus"]
            initial_cost = info_corpus["initial_cost"]

            action = next((cap_to_action[cap] for cap in capabilities if cap in cap_to_action), None)

            if action:
                skill_class = make_remote_skill_class(client, peer_id, action)
                engine.register_skill(peer_id, corpus, skill_class, initial_cost=initial_cost)
                print(f"[Gateway] Registered peer '{peer_id}' with action '{action}' and corpus of size {len(corpus)}")

    # Construct TF-IDF representation and perform initial Cosine K-Means clustering
    engine.initialize_topology(k=2)
    print("[Gateway] Initialized SCSO Engine routing topology with k=2")

    # Implement the Gateway's route action
    async def route_request(req_payload):
        skill_id = req_payload["skill_id"]
        context = req_payload["context"]
        data = req_payload["data"]

        # SCSOEngine parses context, tracks costs, evicts LRU classes and triggers speculative pre-fetching
        proxy_instance = engine.process_request(skill_id, context)

        if proxy_instance is None:
            return {"error": f"Skill {skill_id} could not be resolved or loaded."}

        # Execute the RPC logic in the proxy
        result = await proxy_instance.execute(data)

        # Log gateway telemetry
        pred_skill, utility, _ = engine.router.predict_next(context)
        print(f"  [SCSO Telemetry] Request for '{skill_id}' with context '{context}'. "
              f"Predicted next: '{pred_skill}' (Utility: {utility:.3f}). "
              f"Prefetch threshold: {engine.prefetch_threshold:.3f}. "
              f"Hits/Misses window size: {len(engine.outcome_window)}")
        return result

    client.on_invoke("route_request", route_request)

    # =====================================================================
    # 3. CLIENT WORKLOAD SIMULATION
    # =====================================================================
    print("\n--- Simulation Starting ---")
    requests = [
        {"skill_id": "image_skill", "context": "I want to analyze some camera photographs of objects", "data": {"image_name": "cat.png"}},
        {"skill_id": "code_skill", "context": "Write a python compiler algorithm parser", "data": {"func_name": "parse_ast"}},
        {"skill_id": "sql_skill", "context": "Perform database select tables joined schema postgres", "data": {"query": "SELECT * FROM users"}},
        {"skill_id": "image_skill", "context": "I want to do object detection on a photo, but then immediately I will need python automated coding for compilation of cnn network", "data": {"image_name": "dog.png"}},
    ]

    for idx, req in enumerate(requests):
        print(f"\n[Client] Sending Request #{idx+1} to Gateway: {req['skill_id']} (Context: '{req['context']}')")
        result = await route_request(req)
        print(f"[Client] Received Response: {result}")
        await asyncio.sleep(0.05)

    print("\n--- Simulation Complete ---")
    done_event.set()

async def run_scso_demo():
    hub_task = asyncio.create_task(run_hub())
    await asyncio.sleep(0.15)  # Server bind

    done = asyncio.Event()
    tasks = [
        asyncio.create_task(image_skill_tool()),
        asyncio.create_task(code_skill_tool()),
        asyncio.create_task(sql_skill_tool()),
        asyncio.create_task(scso_gateway_tool(done)),
    ]

    await done.wait()
    await asyncio.sleep(0.1)

    print("\n--- Empirical Hub Relations Log ---")
    print(f"Total recorded relations on Hub: {len(container.relations_log)}")
    for edge in container.relations_log:
        # Sort log entries for cleaner output (or just print as they come)
        pass

    for t in tasks:
        t.cancel()
    hub_task.cancel()
    await asyncio.gather(*tasks, hub_task, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(run_scso_demo())
