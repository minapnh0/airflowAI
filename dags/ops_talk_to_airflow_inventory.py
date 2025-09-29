from __future__ import annotations
from datetime import datetime
from airflow.decorators import dag
import airflow_ai_sdk as ai_task
from pydantic_airflow_agent.agent import build_agent

AGENT = build_agent()

@dag(dag_id="ops_talk_to_airflow_inventory", start_date=datetime(2025,1,1), schedule=None, catchup=False, tags=["ops","ai","talk-to-airflow"])
def ops_talk_to_airflow_inventory():
    AGENT = build_agent()  # reads env vars for creds/model (see agent.py)

    @ai_task.agent(agent=AGENT, result_type=str)  # type: ignore
    def ask_airflow(question: str) -> str:
        return question

    _ = ask_airflow("{{ dag_run.conf.get('question', 'status of inventory_snapshot_ingest') }}")

ops_talk_to_airflow_inventory()
