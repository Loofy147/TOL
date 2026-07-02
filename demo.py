"""
demo.py — proves the container actually works, using 3 stand-in tools:

  solver     — pretends to be an ARC-style solver. Exposes "solve".
  critic     — pretends to score a solution. Exposes "score". Also listens on
               topic "solver_progress" (pub/sub, no direct coupling to solver).
  orchestrator — calls solver.solve, then critic.score with the result, then
                 publishes to "solver_progress".

This exercises all three relation types the hub supports: invoke/result (RPC),
publish/subscribe (broadcast), and the registry/discovery on connect.
At the end it prints the empirical relations_log pulled straight from the hub.
"""
import asyncio
import json
import logging

from hub import main as run_hub, container
from tool_client import ToolClient

logging.getLogger("tool_client").setLevel(logging.WARNING)


async def solver_tool():
    client = ToolClient("solver", capabilities=["solve"])
    await client.connect()

    async def solve(payload):
        # stand-in for a real ARC-AGI solve step
        await asyncio.sleep(0.05)
        return {"grid": [[1, 1], [0, 1]], "task_id": payload["task_id"]}

    client.on_invoke("solve", solve)
    await client.run()


async def critic_tool():
    client = ToolClient("critic", capabilities=["score"])
    await client.connect()
    seen_progress = []

    async def score(payload):
        await asyncio.sleep(0.02)
        return {"score": 0.87, "task_id": payload["task_id"]}

    def on_progress(payload):
        seen_progress.append(payload)
        print(f"    [critic] received solver_progress event: {payload}")

    client.on_invoke("score", score)
    client.on_event("solver_progress", on_progress)
    await client.subscribe("solver_progress")
    await client.run()


async def orchestrator_tool(done_event: asyncio.Event):
    client = ToolClient("orchestrator", capabilities=["orchestrate"])
    await client.connect()
    await asyncio.sleep(0.2)  # let solver/critic register+subscribe first

    print(f"  [orchestrator] discovered peers: {client.peers}")

    solved = await client.invoke("solver", "solve", {"task_id": "arc_017"})
    print(f"  [orchestrator] solver returned: {solved}")

    scored = await client.invoke("critic", "score", solved)
    print(f"  [orchestrator] critic returned: {scored}")

    await client.publish("solver_progress", {"task_id": "arc_017", "score": scored["score"]})
    await asyncio.sleep(0.1)  # let the event land before we tear down

    done_event.set()


async def run_demo():
    hub_task = asyncio.create_task(run_hub())
    await asyncio.sleep(0.15)  # let the server bind

    done = asyncio.Event()
    tools = [
        asyncio.create_task(solver_tool()),
        asyncio.create_task(critic_tool()),
        asyncio.create_task(orchestrator_tool(done)),
    ]

    await done.wait()
    await asyncio.sleep(0.1)

    print("\n--- empirical relations_log (captured from the hub, not designed) ---")
    for edge in container.relations_log:
        print(f"  {edge}")

    for t in tools:
        t.cancel()
    hub_task.cancel()
    await asyncio.gather(*tools, hub_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(run_demo())
