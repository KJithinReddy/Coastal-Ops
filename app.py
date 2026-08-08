"""Minimal Coastal Ops dashboard entrypoint for Databricks Apps."""

from __future__ import annotations

import os

from flask import Flask, jsonify, render_template

app = Flask(__name__)


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok", "service": "coastal-ops-dashboard"})


@app.route("/")
def index():
    # Prefer full UI when template exists; otherwise plain OK.
    try:
        return render_template("index.html")
    except Exception:
        return jsonify({"status": "ok", "service": "coastal-ops-dashboard"})


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", "8000")))
    app.run(host="0.0.0.0", port=port)
