import asyncio
import json
import os
import sys
import time
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.tool_client import ToolClient

async def run_monitor():
    client = ToolClient("monitor", capabilities=["monitor"])
    await client.connect()

    print("TOL Monitor started. Press Ctrl+C to stop.\n")

    try:
        while True:
            # Clear screen (optional, depends on environment)
            # print("\033[H\033[J", end="")

            peers = await client.discovery()

            print(f"--- TOL Status [{datetime.now().strftime('%H:%M:%S')}] ---")
            print(f"{'ID':<15} {'Capabilities':<30} {'Last Seen':<10} {'Status':<10}")
            print("-" * 70)

            now = time.time()
            for p in peers:
                last_seen_diff = now - p.get('last_seen', now)
                status = "OK" if last_seen_diff < 40 else "STALE"
                caps = ", ".join(p.get('capabilities', []))
                print(f"{p['id']:<15} {caps:<30} {int(last_seen_diff):>8}s {status:<10}")

            print("\n")
            await asyncio.sleep(5)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Monitor error: {e}")

if __name__ == "__main__":
    asyncio.run(run_monitor())
