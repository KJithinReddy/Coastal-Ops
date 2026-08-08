# Coastal Ops Copilot

Weather-aware coastal voyage planner: Spark + Open-Meteo/NWS → Lakebase → pgvector RAG → dashboard + MCP agent (Playground → export app).

```
Playground / exported agent  --(MCP)-->  mcp_server/  --(broker)-->  Lakebase + Open-Meteo/NWS
Flask app.py (repo root)  ---------------(same broker)------------>
```

Two Databricks Apps:
- **Dashboard** = repo root (leave Source code path empty)
- **MCP** = `mcp_server/`

## Project layout

```
app.py                            # Flask dashboard
app.yaml
coastal_broker.py
analytics.py
marine_client.py
lakebase.py
templates/index.html
requirements.txt
mcp_server/                       # FastMCP — deploy separately
  coastal_mcp_server.py
  coastal_broker.py               # duplicate (Apps deploy per-folder)
  lakebase.py
  marine_client.py
  app.yaml
  requirements.txt
notebooks/
sql/
setup_secrets.py
README.md
```

## Schema (Lakebase)

**Ops:** `ports`, `voyages`, `marine_snapshots`, `alerts`, `ops_notes`

**RAG:** `marine_documents`, `marine_embeddings` (`vector(384)`, HNSW cosine)

**Analytics:** `app_events` → Delta via the analytics Spark job

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
1. Lakebase + native-password role
2. `python setup_secrets.py` → `database/lakebase-url`
3. This GitHub repo: https://github.com/KJithinReddy/Coastal-Ops

### 1. Deploy dashboard (repo root)
Create app → GitHub → `https://github.com/KJithinReddy/Coastal-Ops` → branch `main`  
**Source code path: leave EMPTY**  
Name: `coastal-ops-dashboard`

### 2. Deploy MCP
Create app → same repo →  
**Source code path: `mcp_server`**  
Name: `coastal-ops-mcp`

### 3. Sync / embed / search
Use the dashboard UI, then run `notebooks/ingest_marine_embeddings.ipynb`.

### 4. Spark + analytics
```bash
python notebooks/spark_marine_pipeline.py
python notebooks/analytics_app_events_to_delta.py
```

### 5. Playground → export agent
Register MCP URL in AI Gateway → Playground → attach tools → export app.

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

Prefer assess_and_act for go/no-go. Only answer with tool data.
```

## Local run

```bash
# Dashboard (:8000)
cd Coastal-Ops
cp .env.example .env
pip install -r requirements.txt
python app.py

# MCP (:8001)
cd mcp_server
cp .env.example .env
pip install -r requirements.txt
PORT=8001 python coastal_mcp_server.py
```

After editing shared modules:

```bash
cp coastal_broker.py lakebase.py marine_client.py mcp_server/
```

## APIs

- [Open-Meteo Marine](https://open-meteo.com/en/docs/marine-weather-api)
- [Open-Meteo Forecast](https://open-meteo.com/en/docs)
- [NWS API](https://www.weather.gov/documentation/services-web-api)
