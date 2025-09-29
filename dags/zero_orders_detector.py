from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Any
import json

from airflow.decorators import dag, task
import airflow_ai_sdk as ai_task
from utils.prompts import build_triage_prompt, ROUTE_PROMPT

ORDERS = [
    {"dc_nbr": 6094, "store_nbr": 100, "item_nbr": 4011, "units": 0,  "order_date": "2025-03-19"},
    {"dc_nbr": 6094, "store_nbr": 101, "item_nbr": 4011, "units": 25, "order_date": "2025-03-19"},
    {"dc_nbr": 6094, "store_nbr": 123, "item_nbr": 4011, "units": 0,  "order_date": "2025-09-28"},
]

@dag(
    dag_id="zero_orders_detector",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=["orders","ai","anomaly"],
    params={"dc_nbr":6094,"order_date":"2025-03-19","item_nbr":4011},
)
def zero_orders_detector():

    @task
    def find(dc_nbr: int | str, order_date: str, item_nbr: int | str) -> List[int]:
        # Airflow will pass templated params as strings; cast to int for comparisons
        dc_nbr = int(dc_nbr)
        item_nbr = int(item_nbr)
        return sorted({
            r["store_nbr"] for r in ORDERS
            if r["dc_nbr"] == dc_nbr and r["order_date"] == order_date
            and r["item_nbr"] == item_nbr and r["units"] == 0
        })

    @ai_task.llm(model="gpt-4o-mini", result_type=dict, temperature=0)
    def triage_zero_orders(stores: List[int], dc_nbr: int, order_date: str, item_nbr: int) -> dict:  # type: ignore
        prompt = build_triage_prompt(json.dumps(
            {"context": {"dc_nbr": dc_nbr, "order_date": order_date, "item_nbr": item_nbr, "stores": stores}},
            ensure_ascii=False
        ))
        return prompt

    @ai_task.llm_branch(
        model="gpt-4o-mini",
        choices=["auto_fix_time_swap", "notify_slack", "assign_ticket"],
        system_prompt=ROUTE_PROMPT,
        temperature=0,
    )
    def route(incident: dict) -> str:  # type: ignore
        return f"{incident}"

    @task
    def auto_fix_time_swap(incident: dict):
        print("[AUTO] Would adjust time-swap / re-drop orders…")

    @task
    def notify_slack(incident: dict):
        print("[SLACK] Zero orders triage:", incident)

    @task
    def assign_ticket(incident: dict):
        print("[ASSIGN] Replenishment Planning")

    stores = find("{{ params.dc_nbr }}", "{{ params.order_date }}", "{{ params.item_nbr }}")
    inc = triage_zero_orders(stores, "{{ params.dc_nbr }}", "{{ params.order_date }}", "{{ params.item_nbr }}")
    decision = route(inc)
    auto_fix_time_swap.set_upstream(decision)
    notify_slack.set_upstream(decision)
    assign_ticket.set_upstream(decision)

zero_orders_detector()
