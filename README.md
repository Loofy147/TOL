# TOL: Tools Online Layer

A minimalist hub-and-spoke framework for connecting autonomous tools and services.

## Architectural Principles

This project follows a clean organization of communication layers and a hub-and-spoke architecture anchored in a central core.

### 10 Layers of Communication
1. **Naming / addressing**: How something is identified (tool_id).
2. **Scheme**: The label for interpretation (e.g., `ws:`, `https:`).
3. **Protocol**: Rule set for exchanging data (TOL Protocol over WebSockets).
4. **Transport**: The carrier (TCP via WebSockets).
5. **Encoding / serialization**: Data shape on the wire (JSON).
6. **Schema**: Structure of the data itself (JSON Schema validation).
7. **Channel / medium**: Path communication uses (WebSocket request/relay).
8. **Communication pattern**: Style of exchange (RPC, Pub/Sub).
9. **Security**: Protection layer (TLS, API Keys).
10. **Topology**: Layout of the system (Hub-and-Spoke).

## Key Features
- **JSON Schema Validation**: Hub enforces data structures between tools.
- **Heartbeats & Health Monitoring**: Automatic cleanup of stale tools and real-time monitoring.
- **Discovery**: Tools can query the registry to find peers and their capabilities.
- **API Key Security**: Simple yet effective authentication for the hub.

## Project Structure
- `core/`: The central hub and client library.
- `services/`: Example tools and specialized services.
- `control/`: Monitoring and management tools.

## Getting Started
1. Install dependencies: `pip install -r requirements.txt`
2. Run the hub: `python3 core/hub.py`
3. Run the monitor: `python3 control/monitor.py`
4. Run the demo: `python3 demo.py`
