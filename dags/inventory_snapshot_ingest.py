from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from airflow.decorators import dag, task
from airflow.models import Variable
import airflow_ai_sdk as ai_task
from utils.prompts import build_triage_prompt, ROUTE_PROMPT

# Optional Slack
try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    SLACK = True
except Exception:
    SLACK = False

ALERT_SLACK_WEBHOOK = Variable.get("ALERT_SLACK_WEBHOOK", default_var=None)
def _slack(msg: str, task_id: str = "inv_snap_slack"):
    if SLACK and ALERT_SLACK_WEBHOOK:
        return SlackWebhookOperator(
            task_id=task_id, http_conn_id=None, webhook_token=ALERT_SLACK_WEBHOOK,
            message=msg, username="airflow-ai-bot"
        )

DEFAULT_ARGS = {"owner": "inventory-team", "retries": 1, "retry_delay": timedelta(minutes=5)}

MOCK_INV = [
    {"item_nbr": 4011, "store_nbr": 123, "dc_nbr": 6094, "on_hand": 35, "safety_stock": 50, "ds": "2025-09-28"},
    {"item_nbr": 4011, "store_nbr": 456, "dc_nbr": 6094, "on_hand": 90, "safety_stock": 40, "ds": "2025-09-28"},
    {"item_nbr": 1111, "store_nbr": 789, "dc_nbr": 6120, "on_hand": 10, "safety_stock": 20, "ds": "2025-09-28"},
]

@dag(
    dag_id="inventory_snapshot_ingest",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["inventory", "ai", "triage"],
)
def inventory_snapshot_ingest():

    @task
    def extract_snapshot(ds: str) -> List[Dict[str, Any]]:
        return [r for r in MOCK_INV if r["ds"] == ds]

    @task
    def transform(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for r in rows:
            r = dict(r)
            r["delta_to_ss"] = r["on_hand"] - r["safety_stock"]
            r["low_inv"] = r["on_hand"] < r["safety_stock"]
            out.append(r)
        return out

    @ai_task.llm(model="gpt-4o-mini", result_type=dict, temperature=0)
    def triage_inventory(rows: List[Dict[str, Any]]) -> dict:  # type: ignore
        lows = [r for r in rows if r.get("low_inv")]
        prompt = build_triage_prompt(json.dumps({"low_inventory": lows}, ensure_ascii=False))
        return prompt

    @ai_task.llm_branch(
        model="gpt-4o-mini",
        choices=["auto_remediate", "notify_slack", "assign_ticket"],
        system_prompt=ROUTE_PROMPT,
        temperature=0,
    )
    def route(incident: dict) -> str:  # type: ignore
        return f"{incident}"

    @task
    def auto_remediate(incident: dict):
        print("[AUTO] Would trigger replenishment flow for low inventory…")

    @task
    def notify_slack(incident: dict):
        text = (":rotating_light: Inventory Triage\n"
                f"Summary: {incident.get('issue_summary')}\n"
                f"Severity: {incident.get('severity')}\n"
                f"Why: {incident.get('justification')}\n"
                f"Next: {incident.get('resolution_steps')}")
        s = _slack(text)
        if s: s.execute(context={})

    @task
    def assign_ticket(incident: dict):
        print(f"[ASSIGN] Team: {incident.get('assigned_team','Inventory Ops')}")

    raw = extract_snapshot("{{ ds }}")
    trn = transform(raw)
    inc = triage_inventory(trn)
    decision = route(inc)
    auto_remediate.set_upstream(decision)
    notify_slack.set_upstream(decision)
    assign_ticket.set_upstream(decision)

inventory_snapshot_ingest()
