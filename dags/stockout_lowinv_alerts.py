from __future__ import annotations
from datetime import datetime
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
def _slack(msg: str, task_id: str = "lowinv_slack"):
    if SLACK and ALERT_SLACK_WEBHOOK:
        return SlackWebhookOperator(task_id=task_id, http_conn_id=None, webhook_token=ALERT_SLACK_WEBHOOK, message=msg, username="airflow-ai-bot")

INV = [
    {"dc_nbr": 6094, "item_nbr": 4011, "on_hand": 90, "daily_demand": 80, "ds": "2025-09-28"},
    {"dc_nbr": 6120, "item_nbr": 4011, "on_hand": 5,  "daily_demand": 20, "ds": "2025-09-28"},
]

@dag(dag_id="stockout_lowinv_alerts", start_date=datetime(2025,1,1), schedule="@daily", catchup=False, tags=["inventory","ai","dos"])
def stockout_lowinv_alerts():

    @task
    def compute(ds: str, threshold_days: float = 1.0) -> List[Dict[str, Any]]:
        rows = [r for r in INV if r["ds"] == ds]
        out = []
        for r in rows:
            dos = (r["on_hand"] / r["daily_demand"]) if r["daily_demand"] else 999
            out.append({**r, "days_of_supply": round(dos, 2), "low": dos < threshold_days})
        return out

    @ai_task.llm(model="gpt-4o-mini", result_type=dict, temperature=0)
    def triage_low_dos(rows: List[Dict[str, Any]]) -> dict:  # type: ignore
        lows = [r for r in rows if r.get("low")]
        prompt = build_triage_prompt(json.dumps({"low_dos": lows}, ensure_ascii=False))
        return prompt

    @ai_task.llm_branch(
        model="gpt-4o-mini",
        choices=["auto_replenish", "notify_slack", "assign_ticket"],
        system_prompt=ROUTE_PROMPT,
        temperature=0,
    )
    def route(incident: dict) -> str:  # type: ignore
        return f"{incident}"

    @task
    def auto_replenish(incident: dict):
        print("[AUTO] Would trigger replenishment DAG…")

    @task
    def notify_slack(incident: dict):
        s = _slack(f":rotating_light: Low DoS triage\n{incident}")
        if s: s.execute(context={})

    @task
    def assign_ticket(incident: dict):
        print("[ASSIGN] Inventory Ops")

    data = compute("{{ ds }}")
    inc = triage_low_dos(data)
    decision = route(inc)
    auto_replenish.set_upstream(decision)
    notify_slack.set_upstream(decision)
    assign_ticket.set_upstream(decision)

stockout_lowinv_alerts()
