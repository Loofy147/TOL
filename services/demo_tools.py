import asyncio
import logging
from core.tool_client import ToolClient

log = logging.getLogger("demo_tools")

async def solver_tool():
    # Example JSON Schema
    schemas = {
        "solve": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"}
            },
            "required": ["task_id"]
        }
    }
    client = ToolClient("solver", capabilities=["solve"], schemas=schemas)
    await client.connect()

    async def solve(payload):
        await asyncio.sleep(0.05)
        return {"grid": [[1, 1], [0, 1]], "task_id": payload["task_id"]}

    client.on_invoke("solve", solve)
    await client.run()


async def critic_tool():
    schemas = {
        "score": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "grid": {"type": "array"}
            },
            "required": ["task_id", "grid"]
        }
    }
    client = ToolClient("critic", capabilities=["score"], schemas=schemas)
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
