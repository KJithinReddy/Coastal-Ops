"""Minimal stub so Databricks Apps can create with an empty source path.
After create, change Git source path to `dashboard` and redeploy (same flow as MCP).
"""

from __future__ import annotations

import os

from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/")
@app.route("/healthz")
def healthz():
    return jsonify(
        {
            "status": "ok",
            "service": "coastal-ops-stub",
            "next_step": "App Settings → set Source code path to 'dashboard' → Deploy",
        }
    )


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", "8000")))
    app.run(host="0.0.0.0", port=port)
