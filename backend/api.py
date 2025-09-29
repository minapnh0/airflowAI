from flask import Flask, jsonify, request
import os

app = Flask(__name__)

# Mock data for DAGs
dags = [
    {"name": "inventory_snapshot_ingest", "status": "running"},
    {"name": "leadtime_anomaly_rca", "status": "success"},
    {"name": "ops_talk_to_airflow_inventory", "status": "failed"},
    {"name": "replenishment_plan_vs_actual", "status": "success"},
    {"name": "stockout_lowinv_alerts", "status": "running"},
    {"name": "zero_orders_detector", "status": "success"}
]

@app.route("/api/dags", methods=["GET"])
def list_dags():
    """Endpoint to list all DAGs and their statuses."""
    return jsonify(dags)

@app.route("/api/dags/trigger", methods=["POST"])
def trigger_dag():
    """Endpoint to trigger a DAG."""
    data = request.json
    dag_name = data.get("dag_name")
    if not dag_name:
        return jsonify({"error": "DAG name is required"}), 400

    # Mock triggering logic
    for dag in dags:
        if dag["name"] == dag_name:
            dag["status"] = "running"
            return jsonify({"message": f"Triggered DAG {dag_name}"})

    return jsonify({"error": "DAG not found"}), 404

@app.route("/api/agent", methods=["POST"])
def interact_with_agent():
    """Endpoint to interact with the Airflow agent."""
    data = request.json
    command = data.get("command")
    if not command:
        return jsonify({"error": "Command is required"}), 400

    # Mock agent response
    response = {"response": f"Agent received command: {command}"}
    return jsonify(response)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
