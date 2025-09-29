import json
from typing import Callable, List, Tuple
import airflow_ai_sdk as ai_task
from utils.prompts import ROUTE_PROMPT, build_triage_prompt

def add_llm_triage_branch(
    task_prefix: str,
    choices: List[str] = None,
) -> Tuple[Callable, Callable]:
    """
    Returns two decorated callables you can use inside any DAG:
      triage(payload: dict) -> dict, route(incident: dict) -> str

    Usage inside a DAG:
      triage, route = add_llm_triage_branch("fillrate", ["auto_remediate","notify_slack","assign_ticket"])
      inc = triage(payload)
      decision = route(inc)
    """
    if choices is None:
        choices = ["auto_remediate", "notify_slack", "assign_ticket"]

    @ai_task.llm(
        task_id=f"{task_prefix}_triage",
        model="gpt-4o-mini",
        result_type=dict
    )
    def triage(payload: dict) -> dict:  # type: ignore
        # Build reusable prompt with strict JSON format + few-shots
        return build_triage_prompt(json.dumps(payload, ensure_ascii=False))

    @ai_task.llm_branch(
        task_id=f"{task_prefix}_route",
        model="gpt-4o-mini",
        choices=choices,
        system_prompt=ROUTE_PROMPT
    )
    def route(incident: dict) -> str:  # type: ignore
        # Pass the triage JSON as the message
        return json.dumps(incident, ensure_ascii=False)

    return triage, route
