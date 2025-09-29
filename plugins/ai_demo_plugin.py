# plugins/ai_demo_plugin.py
from __future__ import annotations
import os, json, asyncio
from datetime import datetime

import httpx
from flask import Blueprint, jsonify, request, Response
from airflow.plugins_manager import AirflowPlugin

# Use the agent you already have
from pydantic_airflow_agent.agent import build_agent

bp = Blueprint("ai_demo", __name__, url_prefix="/ai-demo")

INDEX_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Airflow AI Demo</title>
<style>
 body { font-family: Arial, sans-serif; background:#f4f4f9; margin:0 }
 .container { max-width: 880px; margin: 20px auto; background:#fff; padding:20px; border-radius:8px; box-shadow:0 2px 4px rgba(0,0,0,.1) }
 h1 { text-align:center }
 .row { display:flex; gap:10px; align-items:center; justify-content:space-between; padding:10px 0; border-bottom:1px solid #eee }
 .row:last-child { border-bottom:none }
 .badge { padding:2px 8px; border-radius:12px; font-size:12px; background:#eee }
 .badge.active { background:#e8f5e9 }
 .badge.paused { background:#fff3e0 }
 button { background:#007bff; color:#fff; border:none; padding:6px 10px; border-radius:4px; cursor:pointer }
 button:hover { background:#0056b3 }
 textarea { width:100%; height:110px }
 .muted { color:#666; font-size:13px }
</style>
</head>
<body><div class="container">
  <h1>Airflow AI Demo</h1>

  <h2>DAGs</h2>
  <div id="dags"></div>

  <h2>Agent</h2>
  <p class="muted">Ask in plain English (e.g., “status of inventory_snapshot_ingest” or “trigger replenishment_plan_vs_actual”).</p>
  <textarea id="agentCommand" placeholder="Enter command..."></textarea>
  <button onclick="sendCommand()">Send</button>
  <pre id="agentResponse"></pre>
</div>

<script>
async function fetchDags() {
  const res = await fetch('/ai-demo/api/dags');
  const dags = await res.json();
  const el = document.getElementById('dags');
  el.innerHTML = '';
  dags.forEach(d => {
    const row = document.createElement('div');
    row.className = 'row';
    row.innerHTML = `
      <div><strong>${d.name}</strong> &nbsp;
        <span class="badge ${d.status}">${d.status}</span>
        ${d.latest_state ? '<span class="badge">'+d.latest_state+'</span>' : ''}
      </div>
      <div>
        <button onclick="triggerDag('${d.name}')">Trigger</button>
      </div>`;
    el.appendChild(row);
  });
}

async function triggerDag(name) {
  const res = await fetch('/ai-demo/api/dags/trigger', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ dag_name: name })
  });
  const out = await res.json();
  alert(out.message || out.error || 'done');
  fetchDags();
}

async function sendCommand() {
  const command = document.getElementById('agentCommand').value;
  const res = await fetch('/ai-demo/api/agent', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ command })
  });
  const out = await res.json();
  document.getElementById('agentResponse').textContent = JSON.stringify(out, null, 2);
}

fetchDags();
</script>
</body></html>"""

@bp.get("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")

def _base() -> str:
    base = os.getenv("AIRFLOW_BASE_URI", "http://localhost")
    port = os.getenv("AIRFLOW_API_PORT", "8080")
    return f"{base}:{port}"

def _auth():
    return (os.getenv("AIRFLOW_API_USER", "admin"), os.getenv("AIRFLOW_API_PASS", "admin"))

@bp.get("/api/dags")
def api_dags():
    # List DAGs + quick latest run state
    with httpx.Client(timeout=10.0) as c:
        r = c.get(f"{_base()}/api/v1/dags", auth=_auth())
        r.raise_for_status()
        dags = r.json().get("dags", [])
        out = []
        for d in dags:
            dag_id = d.get("dag_id")
            latest_state = None
            try:
                r2 = c.get(f"{_base()}/api/v1/dags/{dag_id}/dagRuns",
                           params={"order_by":"-logical_date","limit":1},
                           auth=_auth())
                if r2.status_code == 200:
                    runs = r2.json().get("dag_runs", [])
                    if runs:
                        latest_state = runs[0].get("state")
            except Exception:
                pass
            out.append({
                "name": dag_id,
                "status": "paused" if d.get("is_paused") else "active",
                "latest_state": latest_state
            })
        return jsonify(out)

@bp.post("/api/dags/trigger")
def api_trigger():
    data = request.get_json(force=True) or {}
    dag_id = data.get("dag_name")
    if not dag_id:
        return jsonify({"error":"dag_name is required"}), 400
    now = datetime.utcnow().isoformat(timespec="seconds")+"Z"
    body = {"dag_run_id": f"manual__{now}", "logical_date": now, "conf": {}, "note": "AI demo trigger"}
    with httpx.Client(timeout=15.0) as c:
        r = c.post(f"{_base()}/api/v1/dags/{dag_id}/dagRuns", auth=_auth(), json=body)
        if r.status_code >= 400:
            return jsonify({"error": r.text}), r.status_code
    return jsonify({"message": f"Triggered {dag_id}."})

@bp.post("/api/agent")
def api_agent():
    payload = request.get_json(force=True) or {}
    question = payload.get("command") or ""
    agent = build_agent()
    try:
        res = asyncio.run(agent.run(question, deps=agent.default_deps))
        return jsonify({"response": res.data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

class AiDemoPlugin(AirflowPlugin):
    name = "ai_demo"
    flask_blueprints = [bp]
