from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Any
import json

from airflow.decorators import dag, task
import airflow_ai_sdk as ai_task
from utils.prompts import build_triage_prompt, ROUTE_PROMPT

LEADTIMES = [
    {"supplier_id":"S-100","dc_nbr":6094,"days":4},
    {"supplier_id":"S-100","dc_nbr":6094,"days":5},
    {"supplier_id":"S-100","dc_nbr":6094,"days":4},
    {"supplier_id":"S-100","dc_nbr":6094,"days":11},  # spike today
]

@dag(dag_id="leadtime_anomaly_rca", start_date=datetime(2025,1,1), schedule="@daily", catchup=False, tags=["leadtime","ai","triage"])
def leadtime_anomaly_rca():

    @task
    def detect(threshold_pct: float = 0.5) -> List[Dict[str, Any]]:
        base = sum(x["days"] for x in LEADTIMES[:-1]) / (len(LEADTIMES)-1)
        today = LEADTIMES[-1]
        spike = (today["days"]-base)/base if base else 0
        return ([{"supplier_id":today["supplier_id"],"dc_nbr":today["dc_nbr"],"baseline":round(base,2),"today":today["days"],"delta_pct":round(spike,2)}]
                if spike>=threshold_pct else [])

    @ai_task.llm(model="gpt-4o-mini", result_type=dict, temperature=0)
    def triage_leadtime(payload: List[Dict[str, Any]]) -> dict:  # type: ignore
        prompt = build_triage_prompt(json.dumps({"leadtime_spikes": payload}, ensure_ascii=False))
        return prompt

    @ai_task.llm_branch(
        model="gpt-4o-mini",
        choices=["auto_vendor_followup","notify_slack","assign_ticket"],
        system_prompt=ROUTE_PROMPT,
        temperature=0,
    )
    def route(incident: dict) -> str:  # type: ignore
        return f"{incident}"

    @task
    def auto_vendor_followup(incident: dict):
        print("[AUTO] Would open vendor follow-up + transport check…")

    @task
    def notify_slack(incident: dict):
        print("[SLACK] Lead-time triage:", incident)

    @task
    def assign_ticket(incident: dict):
        print("[ASSIGN] Supply Chain PM:", incident)

    flagged = detect()
    inc = triage_leadtime(flagged)
    branch = route(inc)
    auto_vendor_followup.set_upstream(branch)
    notify_slack.set_upstream(branch)
    assign_ticket.set_upstream(branch)

leadtime_anomaly_rca()
