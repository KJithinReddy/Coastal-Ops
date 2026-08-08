# Coastal Ops Copilot

Spark + Open-Meteo/NWS → Lakebase → pgvector RAG → dashboard + MCP (Playground → export).

## Deploy (important)

Create **two** apps from https://github.com/KJithinReddy/Coastal-Ops (`main`):

| App name | Source code path | What it is |
|---|---|---|
| `coastal-ops-dashboard` | **`dashboard`** | Flask UI (sync / search / ops) |
| `coastal-ops-mcp` | **`mcp_server`** | MCP tools for Playground |

Do **not** leave Source code path empty. Empty path = whole repo (nested apps) and creation fails.

After create, Overview → Source must show `/dashboard` or `/mcp_server`. If it does not, delete and recreate.

## Layout

```
dashboard/          # Flask app only
mcp_server/         # FastMCP app only
notebooks/
sql/
setup_secrets.py
README.md
```

## Prerequisites
1. Lakebase + `python setup_secrets.py` → secret `database/lakebase-url`
2. Grant that secret to both apps when Databricks asks

## After both apps are up
1. Dashboard → Sync marine data
2. Run `notebooks/ingest_marine_embeddings.ipynb`
3. Dashboard → Search
4. AI Gateway → add MCP (`coastal-ops-mcp` URL) → Playground → export agent

## Local

```bash
cd dashboard && pip install -r requirements.txt && python app.py
cd mcp_server && pip install -r requirements.txt && PORT=8001 python coastal_mcp_server.py
```

Sync shared modules after edits:

```bash
cp dashboard/coastal_broker.py dashboard/lakebase.py dashboard/marine_client.py mcp_server/
```
