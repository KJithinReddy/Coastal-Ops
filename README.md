# Coastal Ops Copilot

Demo path: **notebooks (sync + embeddings) → MCP → Playground → export agent**.  
No Flask dashboard app required.

```
Spark / notebooks  →  Lakebase (docs + embeddings)
Playground agent   --(MCP)-->  mcp_server/  →  Lakebase + Open-Meteo/NWS
```

## What you need running

| Piece | Status |
|---|---|
| Lakebase + secret `database/lakebase-url` | required |
| Databricks App: **`mcp_server/`** | you already have this |
| Playground → export agent app | demo |
| Flask dashboard | **skip** |

## Demo steps (Databricks)

### 1. Secrets (once)
From the Git folder (`main`):
```bash
python setup_secrets.py
```
Stores `database/lakebase-url`.

### 2. Spark sync (pipeline + Lakebase docs)
Open **`notebooks/run_marine_pipeline.ipynb`** → Run all  

(or in a notebook: import/run `notebooks/spark_marine_pipeline.py` — do not `%run` a notebook with the same name)

This:
- writes Delta → Unity Catalog table `….coastal_ops_marine_conditions` (not `/tmp`, which is blocked when public DBFS is disabled)
- seeds ports, snapshots, and `marine_documents` in Lakebase

### 3. Embeddings (RAG)
Open **`notebooks/ingest_marine_embeddings.ipynb`** → Run all.

### 4. Playground agent
1. AI Gateway → MCP → your **coastal-ops-mcp** URL (already registered if tools show up).
2. Playground → attach MCP tools → paste system prompt below.
3. Try the demo questions.
4. **Export** the agent as a Databricks App when ready.

### System prompt (Playground)

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

### Demo questions
1. `List the coastal ports`
2. `Create a voyage from Miami to Boston named Gulf Stream Run`
3. `Is it safe to sail from Miami? Defer if risky.`
4. `Search marine context for dangerous coastal waves`
5. `List open alerts` / `List voyages`

### 5. Optional analytics Delta
```bash
python notebooks/analytics_app_events_to_delta.py
```
(Main demo proof is MCP tool writes + Spark Delta from step 2.)

```python
display(spark.table("coastal_ops_marine_conditions"))
# or fully qualified: display(spark.table("<catalog>.<schema>.coastal_ops_marine_conditions"))
```

## Layout

```
mcp_server/                         # MCP Databricks App (required)
dashboard/                          # optional Flask UI — not needed for demo
notebooks/spark_marine_pipeline.py  # Spark + Lakebase sync
notebooks/ingest_marine_embeddings.ipynb
sql/
setup_secrets.py
```

## Capstone checklist coverage

| Requirement | How |
|---|---|
| Spark pipeline | `spark_marine_pipeline.py` → Delta |
| Third-party API | Open-Meteo + NWS |
| Unstructured → embeddings | marine narratives → MiniLM → Lakebase |
| Databricks App | MCP app + exported Playground agent |
| Agent read + write | MCP tools create/defer voyages, alerts, notes |
