# Coastal Ops Copilot

Spark + Open-Meteo/NWS → Lakebase → pgvector RAG → dashboard + MCP (Playground → export).

## Deploy (important) — same flow you used for MCP

Custom app always asks for GitHub first. Create with an **empty** source path (repo root has a tiny stub), then **change the folder after create**:

### Dashboard
1. Create app `coastal-ops-dashboard` → repo `Coastal-Ops` → branch `main` → **Source code path EMPTY**
2. Do **not** add App Resources (`lakebase-url`, etc.)
3. Wait until the app is created / compute is active (stub is fine)
4. App **Settings** (or source / Git) → set Source code path to **`dashboard`** → Save → **Deploy**

### MCP (what you already did)
1. Create with empty path (or whatever got it created)
2. Then change Source code path to **`mcp_server`** → Deploy

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
