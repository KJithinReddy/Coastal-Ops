# Coastal Ops Copilot

Spark + Open-Meteo/NWS → Lakebase → pgvector RAG → dashboard + MCP (Playground → export).

## Deploy dashboard (use this — Custom GitHub create keeps failing on `main`)

`main` has both `dashboard/` and `mcp_server/` (two apps). Databricks create is unreliable with that.

Use the **dashboard-only branch**:

1. Delete any failed `coastal-ops-dashboard` app.
2. Create app → Custom → GitHub  
   - Repo: `https://github.com/KJithinReddy/Coastal-Ops`  
   - **Branch: `dashboard-deploy`** (not `main`)  
   - **Source code path: EMPTY**  
   - **Do not** add App Resources  
3. Create / Deploy  
4. After running, grant the app access to secret scope `database` if sync/search fails on DB.

## Deploy MCP

Already working: source path `mcp_server` on branch `main` (your create-then-change-folder flow).

## Layout (`main`)

```
dashboard/          # Flask app
mcp_server/         # FastMCP app
notebooks/
sql/
```

Branch `dashboard-deploy` = dashboard files only at repo root (for Apps create).

## After both apps are up
1. Dashboard → Sync marine data  
2. Run `notebooks/ingest_marine_embeddings.ipynb` (from `main` Git folder)  
3. Search + Playground MCP agent  

## Local

```bash
cd dashboard && pip install -r requirements.txt && python app.py
cd mcp_server && pip install -r requirements.txt && PORT=8001 python coastal_mcp_server.py
```
