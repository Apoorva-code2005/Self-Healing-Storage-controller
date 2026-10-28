"""dashboard/app.py

Self-Healing Storage Controller - Streamlit Dashboard (Phase 7).
Observability, Monitoring, and Failure Injection Control UI.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List
import pandas as pd
import streamlit as st

from dashboard.api_client import ControllerClient, format_bytes, format_risk_level

st.set_page_config(
    page_title="Self-Healing Storage Controller",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)


def render_sidebar() -> tuple[ControllerClient, bool, int]:
    st.sidebar.title("🛡️ Controller Settings")
    api_url = st.sidebar.text_input("Controller API Base URL", value="http://127.0.0.1:8000")
    client = ControllerClient(base_url=api_url)

    # Health probe
    try:
        health = client.get_health()
        st.sidebar.success(
            f"Connected: {health.get('app', 'controller')} v{health.get('version', '')} ({health.get('env', '')})"
        )
    except Exception as exc:
        st.sidebar.error(f"Cannot reach controller: {exc}")

    st.sidebar.markdown("---")
    st.sidebar.subheader("Auto-Refresh")
    auto_refresh = st.sidebar.toggle("Enable Auto-Refresh", value=False)
    interval_s = st.sidebar.slider("Refresh Interval (seconds)", min_value=1, max_value=30, value=3)

    return client, auto_refresh, interval_s


def render_node_overview(client: ControllerClient) -> None:
    st.header("🖥️ Node Health & Virtual Capacity Overview")
    try:
        nodes = client.get_nodes()
    except Exception as exc:
        st.error(f"Error fetching nodes: {exc}")
        return

    if not nodes:
        st.info("No nodes reported by controller.")
        return

    # Metrics summary
    total_nodes = len(nodes)
    healthy_cnt = sum(1 for n in nodes if n.get("status") == "healthy")
    degraded_cnt = sum(1 for n in nodes if n.get("status") == "degraded")
    fenced_failed_cnt = sum(1 for n in nodes if n.get("status") in ("failed", "fenced"))
    total_vcap = sum(n.get("virtual_capacity_bytes", 0) for n in nodes)
    total_free = sum(n.get("free_bytes", 0) for n in nodes)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Nodes", total_nodes)
    c2.metric("Healthy", healthy_cnt)
    c3.metric("Degraded", degraded_cnt)
    c4.metric("Failed / Fenced", fenced_failed_cnt)
    c5.metric("Total Free Capacity", format_bytes(total_free), f"of {format_bytes(total_vcap)}")

    # Detailed table
    rows = []
    for n in nodes:
        status_val = n.get("status", "unknown").upper()
        reachable = "✅ Reachable" if n.get("reachable") else "❌ Unreachable"
        if n.get("fenced"):
            reachable += " (FENCED)"
        rows.append({
            "Node ID": n.get("node_id"),
            "Status": status_val,
            "Connectivity": reachable,
            "Risk Score": f"{float(n.get('risk_score', 0.0)):.2f} ({format_risk_level(n.get('risk_score'))})",
            "Latency (ms)": f"{float(n.get('latency_ms', 0.0)):.1f}",
            "Error Rate": f"{float(n.get('error', 0.0) if isinstance(n.get('error'), (int, float)) else 0.0):.3f}",
            "Free Capacity": format_bytes(n.get("free_bytes")),
            "Virtual Quota": format_bytes(n.get("virtual_capacity_bytes")),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True)


def render_predictions(client: ControllerClient) -> None:
    st.header("🔮 Risk Predictions & Score Breakdown")
    try:
        preds = client.get_predictions(limit=25)
    except Exception as exc:
        st.error(f"Error fetching predictions: {exc}")
        return

    col_weights, col_preds = st.columns([1, 3])

    with col_weights:
        st.subheader("Configured Weights")
        try:
            prio_resp = client.get_priority_objects(limit=1)
            weights = prio_resp.get("weights", {})
            st.write(f"- **Criticality (w1)**: `{weights.get('w1_criticality', 0.40)}`")
            st.write(f"- **Access Freq (w2)**: `{weights.get('w2_access_frequency', 0.20)}`")
            st.write(f"- **Node Risk (w3)**: `{weights.get('w3_node_risk', 0.25)}`")
            st.write(f"- **Redundancy Vuln (w4)**: `{weights.get('w4_redundancy', 0.15)}`")
        except Exception:
            st.info("Default formula weights: w1=0.4, w2=0.2, w3=0.25, w4=0.15")

    with col_preds:
        st.subheader("Latest Predictions & Anomaly Flags")
        if not preds:
            st.info("No prediction records found.")
        else:
            p_rows = []
            for p in preds:
                flag = "⚠️ ANOMALY" if p.get("anomaly_flag") else "Normal"
                p_rows.append({
                    "Node ID": p.get("node_id"),
                    "Risk Score": f"{float(p.get('risk_score', 0.0)):.2f}",
                    "Status": flag,
                    "Model": f"{p.get('model_name')} v{p.get('model_version')}",
                    "Explanation": p.get("explanation"),
                    "Timestamp": p.get("timestamp_utc"),
                })
            st.dataframe(pd.DataFrame(p_rows), use_container_width=True)


def render_priority_ranked(client: ControllerClient) -> None:
    st.header("📊 Priority-Ranked Objects")
    col_filters = st.columns(3)
    with col_filters[0]:
        filter_node = st.text_input("Filter by Node ID (optional)", value="")
    with col_filters[1]:
        gamma_val = st.slider("Size Exponent (gamma)", min_value=0.0, max_value=1.0, value=0.0, step=0.1)
    with col_filters[2]:
        use_rate_val = st.checkbox("Use PriorityRate (Size-Aware)", value=False)

    try:
        prio_data = client.get_priority_objects(
            node_id=filter_node.strip() if filter_node.strip() else None,
            gamma=gamma_val,
            use_rate=use_rate_val,
            limit=50,
        )
        items = prio_data.get("items", [])
    except Exception as exc:
        st.error(f"Error fetching priority ranked objects: {exc}")
        return

    st.write(f"Total Ranked Objects: **{prio_data.get('total', len(items))}**")
    if not items:
        st.info("No objects match criteria.")
        return

    obj_rows = []
    for obj in items:
        breakdown = obj.get("priority_breakdown", {})
        obj_rows.append({
            "Object ID": obj.get("object_id"),
            "Name": obj.get("object_name"),
            "Size": format_bytes(obj.get("size_bytes")),
            "Criticality": obj.get("criticality"),
            "Access Norm": f"{float(obj.get('access_frequency_normalized', 0.0)):.3f}",
            "Priority Score": f"{float(obj.get('priority', 0.0)):.3f}",
            "Score Breakdown": f"C:{breakdown.get('criticality', 0):.2f} A:{breakdown.get('access', 0):.2f} N:{breakdown.get('node_risk', 0):.2f} R:{breakdown.get('redundancy', 0):.2f}",
            "Replicas": len(obj.get("replicas", [])),
            "Status": obj.get("status"),
        })
    st.dataframe(pd.DataFrame(obj_rows), use_container_width=True)


def render_recovery_status(client: ControllerClient) -> None:
    st.header("🔄 Migration & Recovery Status")
    try:
        rec_data = client.get_recovery_status(limit=50)
    except Exception as exc:
        st.error(f"Error fetching recovery status: {exc}")
        return

    summary = rec_data.get("summary", {})
    jobs = rec_data.get("jobs", [])
    lost_objects = rec_data.get("lost_objects", [])

    # Lost objects alert
    if lost_objects:
        st.error(
            f"🚨 **ALERT: {len(lost_objects)} Lost Objects Detected!** (Zero surviving replicas after hard failure): {', '.join(lost_objects)}"
        )

    # Job counts metrics
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Queued", summary.get("queued", 0))
    c2.metric("Running", summary.get("running", 0))
    c3.metric("Verifying", summary.get("verifying", 0))
    c4.metric("Succeeded", summary.get("succeeded", 0))
    c5.metric("Blocked", summary.get("blocked", 0))
    c6.metric("Failed", summary.get("failed", 0))

    if not jobs:
        st.info("No recent recovery or evacuation jobs.")
        return

    job_rows = []
    for j in jobs:
        job_rows.append({
            "Job ID": j.get("job_id"),
            "Object ID": j.get("object_id"),
            "Status": j.get("status"),
            "Source Node": j.get("source_node_id"),
            "Target Node": j.get("target_node_id") or "Pending",
            "Priority Score": f"{float(j.get('priority_score') or 0.0):.2f}",
            "Bytes Moved": format_bytes(j.get("bytes_transferred")),
            "Retries": j.get("retries", 0),
            "SHA-256 Checksum": j.get("verification_result") or "N/A",
            "Redundancy Status": j.get("redundancy_status"),
            "Error Message": j.get("error_message") or "",
        })
    st.dataframe(pd.DataFrame(job_rows), use_container_width=True)


def render_failure_injection_controller(client: ControllerClient) -> None:
    st.header("⚡ Failure Injection & Recovery Control")
    col_sim, col_rec = st.columns(2)

    with col_sim:
        st.subheader("Simulate Failure Scenario")
        scenario_name = st.selectbox(
            "Scenario Name",
            options=["true_alarm", "false_alarm", "missed_prediction"],
            index=0,
        )
        advance_ticks = st.number_input("Advance Ticks (Clock Steps)", min_value=1, max_value=100, value=5)
        seed_val = st.number_input("PRNG Seed", min_value=0, max_value=99999, value=42)

        if st.button("🚀 Trigger Failure Simulation", type="primary"):
            try:
                res = client.simulate_failure(
                    scenario_name=scenario_name,
                    advance_ticks=int(advance_ticks),
                    seed=int(seed_val),
                )
                st.success(f"Simulation completed! Ticks advanced: {res.get('ticks_advanced')}")
                st.json(res)
            except Exception as exc:
                st.error(f"Simulation failed: {exc}")

    with col_rec:
        st.subheader("Trigger Recovery / Evacuation")
        target_node = st.text_input("Node ID to Recover", value="node-1")
        rec_mode = st.radio("Recovery Ordering Mode", options=["priority", "fifo"], horizontal=True)

        if st.button("🛡️ Trigger Recovery", type="primary"):
            try:
                rec_res = client.trigger_recovery(node_id=target_node.strip(), mode=rec_mode)
                jobs = rec_res.get("jobs", [])
                st.success(f"Recovery initiated! Created {len(jobs)} jobs in {rec_mode} mode.")
                st.json(rec_res)
            except Exception as exc:
                st.error(f"Recovery request failed: {exc}")


def main() -> None:
    client, auto_refresh, interval_s = render_sidebar()

    st.title("🛡️ Self-Healing Storage Controller Dashboard")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🖥️ Nodes",
        "🔮 Predictions",
        "📊 Ranked Objects",
        "🔄 Recovery & Jobs",
        "⚡ Controller Actions",
    ])

    with tab1:
        render_node_overview(client)
    with tab2:
        render_predictions(client)
    with tab3:
        render_priority_ranked(client)
    with tab4:
        render_recovery_status(client)
    with tab5:
        render_failure_injection_controller(client)

    if auto_refresh:
        time.sleep(interval_s)
        st.rerun()


if __name__ == "__main__":
    main()
