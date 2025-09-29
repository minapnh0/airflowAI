from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from airflow.decorators import dag, task
from airflow.models import Variable
import airflow_ai_sdk as ai_task
from utils.prompts import build_triage_prompt, ROUTE_PROMPT

try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    SLACK = True
except Exception:
    SLACK = False

ALERT_SLACK_WEBHOOK = Variable.get("ALERT_SLACK_WEBHOOK", default_var=None)
def _slack(msg: str, task_id: str = "plan_actual_slack"):
    if SLACK and ALERT_SLACK_WEBHOOK:
        return SlackWebhookOperator(task_id=task_id, http_conn_id=None, webhook_token=ALERT_SLACK_WEBHOOK, message=msg, username="airflow-ai-bot")

DEFAULT_ARGS = {"owner": "inventory-team", "retries": 1, "retry_delay": timedelta(minutes=5)}

PLAN = [
    {"dc_nbr": 6094, "store_nbr": 123, "item_nbr": 4011, "planned_units": 120, "ds": "2025-09-28"},
    {"dc_nbr": 6094, "store_nbr": 456, "item_nbr": 4011, "planned_units": 80,  "ds": "2025-09-28"},
]
ACTUAL = [
    {"dc_nbr": 6094, "store_nbr": 123, "item_nbr": 4011, "shipped_units": 90,  "ds": "2025-09-28"},
    {"dc_nbr": 6094, "store_nbr": 456, "item_nbr": 4011, "shipped_units": 80,  "ds": "2025-09-28"},
]

@dag(dag_id="replenishment_plan_vs_actual", start_date=datetime(2025,1,1), schedule="@daily", catchup=False, default_args=DEFAULT_ARGS, tags=["orders","ai","triage"])
def plan_vs_actual():

    @task
    def fetch_plan(ds: str) -> List[Dict[str, Any]]:
        return [r for r in PLAN if r["ds"] == ds]

    @task
    def fetch_actual(ds: str) -> List[Dict[str, Any]]:
        return [r for r in ACTUAL if r["ds"] == ds]

    @task
    def compare(plan: List[Dict[str, Any]], actual: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        idx = {(r["dc_nbr"], r["store_nbr"], r["item_nbr"]): r for r in actual}
        out = []
        for p in plan:
            a = idx.get((p["dc_nbr"], p["store_nbr"], p["item_nbr"]), {"shipped_units": 0})
            fill = (a["shipped_units"] / p["planned_units"]) if p["planned_units"] else 1.0
            out.append({**p, "shipped_units": a["shipped_units"], "fill_rate": round(fill, 3)})
        return out

    @ai_task.llm(model="gpt-4o-mini", result_type=dict, temperature=0)
    def triage_fill_rate(rows: List[Dict[str, Any]]) -> dict:  # type: ignore
        lows = [r for r in rows if r["fill_rate"] < 0.90]
        prompt = build_triage_prompt(json.dumps({"low_fill": lows}, ensure_ascii=False))
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
        print("[AUTO] Would schedule catch-up shipment / re-dispatch…")

    @task
    def notify_slack(incident: dict):
        s = _slack(f":warning: Fill-rate triage\n{incident}")
        if s: s.execute(context={})

    @task
    def assign_ticket(incident: dict):
        print(f"[ASSIGN] {incident.get('assigned_team','Vendor Relations')}")

    p = fetch_plan("{{ ds }}"); a = fetch_actual("{{ ds }}"); cmp = compare(p, a)
    inc = triage_fill_rate(cmp)
    decision = route(inc)
    auto_remediate.set_upstream(decision)
    notify_slack.set_upstream(decision)
    assign_ticket.set_upstream(decision)

plan_vs_actual()
