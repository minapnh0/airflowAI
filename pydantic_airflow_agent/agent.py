from __future__ import annotations
import os, json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import httpx
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.vertexai import VertexAIModel

# ---- Optional pretty logs (safe if colorlog not installed) -------------------
try:
    import logging, colorlog
    log_format = '%(log_color)s%(asctime)s [%(levelname)s] %(reset)s[%(name)s] %(message)s'
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(log_format))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    logger = logging.getLogger("pydantic_airflow_agent")
except Exception:  # pragma: no cover
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("pydantic_airflow_agent")

# ---- Deps passed to tools ----------------------------------------------------
@dataclass
class Deps:
    airflow_api_base_uri: str
    airflow_api_port: int
    airflow_api_user: str
    airflow_api_pass: str

# ---- Result shape used by the agent -----------------------------------------
class DAGStatus(BaseModel):
    dag_id: str = Field(description="ID of the DAG")
    dag_display_name: Optional[str] = Field(default=None, description="Display name of the DAG")
    is_paused: Optional[bool] = Field(default=None, description="Whether the DAG is paused")
    next_dagrun_data_interval_start: Optional[str] = Field(default=None, description="Next DAG run data interval start")
    next_dagrun_data_interval_end: Optional[str] = Field(default=None, description="Next DAG run data interval end")
    last_dag_run_id: Optional[str] = Field(default=None, description="Last DAG run ID")
    last_dag_run_state: Optional[str] = Field(default=None, description="Last DAG run state")
    total_dag_runs: Optional[int] = Field(default=None, description="Total number of DAG runs")

# ---- Build the agent used by the DAG (@task.agent) ---------------------------
def build_agent() -> Agent[Deps, DAGStatus]:
    model = VertexAIModel(
        model_name=os.getenv("LLM_MODEL", "gemini-2.0-flash"),
        service_account_file=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp-credentials.json"),
        # temperature stays low by default
    )

    agent = Agent(
        model=model,
        system_prompt=(
            "You are an Airflow monitoring assistant. "
            "Use the provided tools to list DAGs, fetch a DAG's status, or trigger a DAG run. "
            "When the user asks about a single DAG, return a JSON that matches the DAGStatus schema. "
            "If the user asks for 'list', call list_dags and summarize key DAG ids; "
            "if they ask to trigger, call trigger_dag. Be concise."
        ),
        result_type=DAGStatus,
        deps_type=Deps,
        retries=1,
    )

    # ---- Tools ----------------------------------------------------------------

    @agent.tool
    async def list_dags(ctx: RunContext[Deps]) -> str:
        """Return basic info for all DAGs (id, display_name, description) as JSON list."""
        logger.info("Listing DAGs…")
        base = f"{ctx.deps.airflow_api_base_uri}:{ctx.deps.airflow_api_port}/api/v1/dags"
        auth = (ctx.deps.airflow_api_user, ctx.deps.airflow_api_pass)
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(base, auth=auth)
            r.raise_for_status()
            dags = r.json().get("dags", [])
            return json.dumps(
                [{k: d.get(k) for k in ("dag_id", "dag_display_name", "description")} for d in dags]
            )

    @agent.tool
    async def get_dag_status(ctx: RunContext[Deps], dag_id: str) -> str:
        """Get core status for a single DAG and its latest run."""
        logger.info(f"Getting status for DAG: {dag_id}")
        base = f"{ctx.deps.airflow_api_base_uri}:{ctx.deps.airflow_api_port}/api/v1"
        auth = (ctx.deps.airflow_api_user, ctx.deps.airflow_api_pass)

        async with httpx.AsyncClient(timeout=30.0) as client:
            d = await client.get(f"{base}/dags/{dag_id}", auth=auth)
            d.raise_for_status()
            # Airflow >= 2.7 uses logical_date (not execution_date)
            runs = await client.get(
                f"{base}/dags/{dag_id}/dagRuns",
                params={"order_by": "-logical_date", "limit": 1},
                auth=auth,
            )
            runs.raise_for_status()

        dag = d.json()
        runs_json = runs.json()
        latest = (runs_json.get("dag_runs") or [None])[0]

        result = {
            "dag_id": dag.get("dag_id"),
            "dag_display_name": dag.get("dag_display_name"),
            "is_paused": dag.get("is_paused"),
            "next_dagrun_data_interval_start": dag.get("next_dagrun_data_interval_start"),
            "next_dagrun_data_interval_end": dag.get("next_dagrun_data_interval_end"),
            "last_dag_run_id": (latest or {}).get("dag_run_id"),
            "last_dag_run_state": (latest or {}).get("state"),
            "total_dag_runs": runs_json.get("total_entries"),
        }
        logger.debug("DAG status payload: %s", json.dumps(result))
        return json.dumps(result)

    @agent.tool
    async def trigger_dag(ctx: RunContext[Deps], dag_id: str) -> str:
        """Trigger a DAG run now."""
        logger.info(f"Triggering DAG: {dag_id}")
        base = f"{ctx.deps.airflow_api_base_uri}:{ctx.deps.airflow_api_port}/api/v1"
        auth = (ctx.deps.airflow_api_user, ctx.deps.airflow_api_pass)
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        payload = {
            "dag_run_id": f"manual__{now}",
            "logical_date": now,              # modern field; execution_date is legacy
            "conf": {},
            "note": "Triggered by AI Agent",
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(f"{base}/dags/{dag_id}/dagRuns", json=payload, auth=auth)
            r.raise_for_status()
            return json.dumps(r.json())

    # Default deps from env so the DAG can call without passing anything
    agent.default_deps = Deps(
        airflow_api_base_uri=os.getenv("AIRFLOW_BASE_URI", "http://webserver"),
        airflow_api_port=int(os.getenv("AIRFLOW_API_PORT", "8080")),
        airflow_api_user=os.getenv("AIRFLOW_API_USER", "admin"),
        airflow_api_pass=os.getenv("AIRFLOW_API_PASS", "admin"),
    )
    return agent
