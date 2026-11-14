# 2. Standard-library HTTP server

Status: accepted, revisit at the trigger below

## Context
The API does four things: authenticate, check backpressure, mask, and copy rows into the inbox. Most of the time is spent in Postgres, not in HTTP parsing.

## Decision
Use `ThreadingHTTPServer` with no framework, and add explicitly what a framework or proxy would supply:
- a 15-second socket timeout, so a client that stalls mid-request loses its connection;
- a cap on requests in flight (`CONVOFLOW_MAX_IN_FLIGHT`), answered with 503 and `Retry-After`;
- a body size limit, answered with 413;
- queue backpressure (`CONVOFLOW_MAX_QUEUE`), answered with 429 and `Retry-After`.
On AWS an ALB sits in front and terminates TLS.

## Consequences
- Two runtime dependencies (psycopg and its pool), plus OpenTelemetry. Easy to audit.
- No request validation library or OpenAPI schema. Payload shapes live in `adapters.py`.
- Trigger to move to an ASGI framework (Starlette or FastAPI on uvicorn): needing streaming uploads, websockets, more than about 20 routes, or a generated client SDK.
