# Coastal Ops Copilot

Weather-aware coastal voyage planner: Spark + Open-Meteo/NWS → Lakebase → pgvector RAG → dashboard + MCP agent (Playground → export app).

```
Playground / exported agent  --(MCP)-->  mcp_server/  --(broker)-->  Lakebase + Open-Meteo/NWS
dashboard/app.py  ------------------------(same broker)------------>
```

`mcp_server/` and `dashboard/` are two separate Databricks Apps (same split as HW3). Broker / lakebase / marine_client are duplicated per folder because Apps deploy from a single directory.

## Project layout

```
mcp_server/
  coastal_mcp_server.py       # FastMCP tools
  coastal_broker.py
  lakebase.py
  marine_client.py
  app.yaml
  requirements.txt
dashboard/
  app.py                      # Flask: sync, search, voyages, alerts
  coastal_broker.py
  lakebase.py
  marine_client.py
  analytics.py
  templates/index.html
  app.yaml
  requirements.txt
notebooks/spark_marine_pipeline.py
notebooks/analytics_app_events_to_delta.py
notebooks/ingest_marine_embeddings.py
sql/
setup_secrets.py
README.md
```

## Schema (Lakebase)

**Ops:** `ports`, `voyages`, `marine_snapshots`, `alerts`, `ops_notes`

**RAG:** `marine_documents`, `marine_embeddings` (`vector(384)`, HNSW cosine)

**Analytics:** `app_events` → Delta via the analytics Spark job

**Chunking:** `CHUNK_SIZE=800`, `CHUNK_OVERLAP=100`

## Agent tools (MCP)

| Tool | Type | What it does |
|---|---|---|
| `list_ports` / `get_port_conditions` | read (+ snapshot write) | Live marine risk at a port |
| `search_marine_context` | read | Semantic RAG over forecasts/alerts |
| `list_voyages` / `list_alerts` | read | Ops state |
| `create_voyage` | write | Schedule a voyage |
| `reschedule_voyage` | write | Defer a voyage + alert |
| `create_alert` / `save_ops_note` | write | Persist ops actions |
| `assess_and_act` | read+write | Go/no-go: alert + defer if high/severe |

---

## End-to-end (Databricks)

### 0. Prerequisites
1. Lakebase instance with a native-password role
2. `python setup_secrets.py` once → stores `database/lakebase-url`
3. Git folder for this Capstone repo

### 1. Tables (optional — apps also auto-create)
```text
sql/01_setup_ops_tables.sql
sql/02_setup_marine_documents.sql
sql/03_setup_marine_embeddings.sql
sql/04_setup_app_events.sql
```

### 2. Deploy dashboard
Compute → Apps → Create app → source = `…/Capstone/dashboard/`. Deploy.

### 3. Spark pipeline
```bash
python notebooks/spark_marine_pipeline.py
```
Writes Delta to `/tmp/coastal_ops/marine_conditions_silver` and syncs into Lakebase.

### 3b. App analytics → Delta
After using the dashboard, run:
```bash
python notebooks/analytics_app_events_to_delta.py
```

```python
display(spark.read.format("delta").load("/tmp/coastal_ops/app_analytics_daily"))
```

### 4. Sync marine data
```bash
curl -X POST https://<dashboard-url>/marine/sync \
  -H "Content-Type: application/json" \
  -d '{"locations": ["Miami, FL", "Boston, MA", "Seattle, WA"], "limit": 40}'
```

### 5. Embed

**Databricks:** open `notebooks/ingest_marine_embeddings.ipynb` → Run all.

**CLI:**
```bash
pip install -r dashboard/requirements.txt
python notebooks/ingest_marine_embeddings.py
```

### 6. Search
```bash
curl -X POST https://<dashboard-url>/marine/search \
  -H "Content-Type: application/json" \
  -d '{"query": "high waves and dangerous coastal conditions", "top_k": 5}'
```

### 7. Deploy MCP → Playground → export app

1. Create a second Databricks App pointed at `…/Capstone/mcp_server/`. Deploy and copy the URL.
2. **AI Gateway → MCPs → Add MCP** — paste the MCP app URL (streamable HTTP), name e.g. `coastal-ops`.
3. Open **Playground**, attach the `coastal-ops` MCP tools, paste the system prompt below, and try the demo questions.
4. **Export the agent as a Databricks App** from Playground when it looks good.

#### System prompt (Playground)

```
You are Coastal Ops Copilot. You answer marine voyage questions and take real
actions in Lakebase (create/reschedule voyages, alerts, ops notes).

Tools (call these — never invent wave heights, wind, or alerts):
1. list_ports — known coastal ports.
2. get_port_conditions(port_name) — live marine + wind; stores a snapshot.
3. search_marine_context(query, top_k) — semantic RAG over forecasts/alerts.
4. list_voyages / list_alerts — current ops state.
5. create_voyage(name, origin_port, dest_port, …) — schedule a voyage (write).
6. reschedule_voyage(voyage_id, …) — defer a voyage + warning alert (write).
7. create_alert / save_ops_note — persist ops actions (write).
8. assess_and_act(port_name, voyage_id) — go/no-go; if risk is high/severe,
   creates an alert and optionally defers the voyage.

Tool-calling order:
- "What's the sea like at Miami?" → get_port_conditions (and search_marine_context if useful).
- "Schedule Miami → Boston…" → create_voyage.
- "Is it safe to sail? Defer if risky." → assess_and_act (pass voyage_id when known).
- "Any open alerts / voyages?" → list_alerts / list_voyages.

Guardrails:
- Only answer with tool data. If status=error, tell the user and ask to clarify.
- Prefer assess_and_act for go/no-go demos.
- Keep answers concise: recommendation first, then 2–4 supporting facts.
```

#### Demo questions (Playground)
- Create a voyage from Miami to Boston named Gulf Stream Run
- Is it safe to sail from Miami? Defer if risky.
- What's the sea like at Seattle right now?

## Local run

```bash
# Terminal 1 — dashboard (:8000)
cd Capstone/dashboard
cp .env.example .env   # paste LAKEBASE_URL
pip install -r requirements.txt
python app.py

# Terminal 2 — MCP server (:8001)
cd Capstone/mcp_server
cp .env.example .env
pip install -r requirements.txt
PORT=8001 python coastal_mcp_server.py
```

After changing shared modules, re-copy both ways (or edit one side and copy):

```bash
# from Capstone/
cp dashboard/coastal_broker.py dashboard/lakebase.py dashboard/marine_client.py mcp_server/
# or the reverse if you edited mcp_server first
```

## Demo script

1. Open the dashboard → Health
2. Sync marine data for Miami / Boston / Seattle
3. Confirm embeddings notebook / Spark pipeline was run
4. Search “dangerous coastal waves”
5. Playground (MCP): create a voyage, then go/no-go assess — export the agent app when ready
6. Refresh dashboard Voyages / Open alerts
7. Show Delta analytics: `display(spark.read.format("delta").load("/tmp/coastal_ops/app_analytics_daily"))`

## APIs

- [Open-Meteo Marine](https://open-meteo.com/en/docs/marine-weather-api) — waves, swell (no key)
- [Open-Meteo Forecast](https://open-meteo.com/en/docs) — wind
- [NWS API](https://www.weather.gov/documentation/services-web-api) — coastal/marine alerts (User-Agent only)
