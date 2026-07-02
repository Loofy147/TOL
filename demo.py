import asyncio
import logging
import os

from core.hub import main as run_hub, container
from core.tool_client import ToolClient
from services.demo_tools import solver_tool, critic_tool

logging.getLogger("tool_client").setLevel(logging.WARNING)

async def orchestrator_tool(done_event: asyncio.Event):
    client = ToolClient("orchestrator", capabilities=["orchestrate"])
    await client.connect()
    await asyncio.sleep(0.2)

    print(f"  [orchestrator] discovered peers: {len(client.peers)}")

    # Discovery test
    all_peers = await client.discovery()
    print(f"  [orchestrator] discovery returned {len(all_peers)} total peers")

    # Success case
    solved = await client.invoke("solver", "solve", {"task_id": "arc_017"})
    print(f"  [orchestrator] solver returned valid: {solved}")

    # Validation failure case (invalid payload for solver)
    print("  [orchestrator] testing schema validation failure...")
    try:
        await client.invoke("solver", "solve", {"wrong_field": 123})
        print("  Error: solver accepted invalid payload!")
    except RuntimeError as e:
        print(f"  [orchestrator] caught expected validation error: {e}")

    scored = await client.invoke("critic", "score", solved)
    print(f"  [orchestrator] critic returned: {scored}")

    await client.publish("solver_progress", {"task_id": "arc_017", "score": scored["score"]})
    await asyncio.sleep(0.1)

    done_event.set()


async def run_demo():
    hub_task = asyncio.create_task(run_hub())
    await asyncio.sleep(0.15)

    done = asyncio.Event()
    tools = [
        asyncio.create_task(solver_tool()),
        asyncio.create_task(critic_tool()),
        asyncio.create_task(orchestrator_tool(done)),
    ]

    await done.wait()
    await asyncio.sleep(0.1)

    print("\n--- empirical relations_log ---")
    for edge in container.relations_log:
        print(f"  {edge}")

    for t in tools:
        t.cancel()
    hub_task.cancel()
    await asyncio.gather(*tools, hub_task, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(run_demo())
