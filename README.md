# TOL: Tools Online Layer

A minimalist hub-and-spoke framework for connecting autonomous tools and services.

## Architectural Principles

This project follows a clean organization of communication layers and a hub-and-spoke architecture anchored in a central core.

### 10 Layers of Communication
1. **Naming / addressing**: How something is identified (e.g., tool_id).
2. **Scheme**: The label for interpretation (e.g., `ws:`, `https:`).
3. **Protocol**: Rule set for exchanging data (TOL Protocol over WebSockets).
4. **Transport**: The carrier (TCP via WebSockets).
5. **Encoding / serialization**: Data shape on the wire (JSON).
6. **Schema**: Structure of the data itself (JSON Schema).
7. **Channel / medium**: Path communication uses (WebSocket request/relay).
8. **Communication pattern**: Style of exchange (RPC, Pub/Sub).
9. **Security**: Protection layer (TLS, API Keys).
10. **Topology**: Layout of the system (Hub-and-Spoke).

### Clean Architecture (Hub-and-Spoke)
- **Core layer**: One main region (e.g., AWS Paris) holds the Hub, API, and main DB.
- **Service layer**: Separate services for compute, data, AI, and background jobs.
- **Edge layer**: CDN, caching, and public gateway in front of the core.
- **Adapter layer**: Connectors to outside systems (payments, email, model APIs).
- **Control layer**: Logging, monitoring, secrets, and policy management.

## Project Structure
- `core/`: The central hub and client library.
- `services/`: Example tools and specialized services.
- `control/`: Monitoring and management tools.

## Getting Started
1. Install dependencies: `pip install websockets`
2. Run the demo: `python3 demo.py`
