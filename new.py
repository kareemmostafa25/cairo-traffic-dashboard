"""
Cairo Traffic Dashboard (Streamlit)
Supports: Local Simulator mode (built-in), + placeholders for Event Hub/Blob/SQL.
Features:
- Overview / Map / Analytics / Alerts tabs
- Live refresh (1s or 5s)
- Anomaly detection (speed, congestion, incidents)
- Congestion heatmap (pydeck)
- Vehicle-type distribution, avg speed, realtime counts
"""

import streamlit as st
import pandas as pd
import numpy as np
import time
import random
from datetime import datetime, timezone, timedelta
import pydeck as pdk
import plotly.express as px

# Optional: uncomment if you want to integrate Azure Event Hub later
# from azure.eventhub import EventHubConsumerClient

# ----------------------------
# Settings / Constants
# ----------------------------
SIM_DEFAULT_INTERVAL = 5  # seconds
MAX_EVENTS_KEEP = 5000    # keep recent N events in memory to avoid memory blowup

LOCATIONS = [
    {"id": "LOC001", "name": "Tahrir Square", "lat": 30.0444, "lon": 31.2357, "cap": 120},
    {"id": "LOC002", "name": "Ramses Square", "lat": 30.0626, "lon": 31.2497, "cap": 150},
    {"id": "LOC003", "name": "6th October Bridge", "lat": 30.0626, "lon": 31.2444, "cap": 100},
    {"id": "LOC004", "name": "Nasr City - Abbas El Akkad", "lat": 30.0515, "lon": 31.3381, "cap": 80},
    {"id": "LOC005", "name": "Heliopolis - Uruba Street", "lat": 30.0808, "lon": 31.3239, "cap": 90},
    {"id": "LOC006", "name": "Maadi Corniche", "lat": 29.9594, "lon": 31.2584, "cap": 60},
    {"id": "LOC007", "name": "Ahmed Orabi Square", "lat": 30.0618, "lon": 31.2001, "cap": 110},
]

VEHICLE_TYPES = ["Car", "Taxi", "Bus", "Microbus", "Truck", "Motorcycle", "Delivery Van"]
WEATHER_CONDITIONS = ["Clear", "Cloudy", "Light Rain", "Heavy Rain", "Foggy", "Sandstorm"]
TRAFFIC_INCIDENTS = ["None", "Minor Accident", "Major Accident", "Vehicle Breakdown", "Road Construction", "Police Checkpoint"]

# ----------------------------
# Helpers: Traffic generation & anomaly detection
# ----------------------------
def calculate_rush_hour_factor(dt=None):
    if dt is None:
        dt = datetime.now()
    hour = dt.hour
    if 7 <= hour <= 10:
        return 1.5
    elif 18 <= hour <= 21:
        return 1.4
    elif hour <= 6 or hour >= 22:
        return 0.4
    else:
        return 1.0

def generate_realistic_traffic_data(now=None):
    location = random.choice(LOCATIONS)
    if now is None:
        now = datetime.now(timezone.utc)
    rush_factor = calculate_rush_hour_factor(now.astimezone())
    cap = location["cap"]
    min_vehicles = max(5, int(cap * 0.3 * rush_factor))
    max_vehicles = min(cap, int(cap * 1.2 * rush_factor))
    vehicle_count = random.randint(min_vehicles, max_vehicles)

    # occasional congestion spike
    if random.random() < 0.05:
        vehicle_count = int(min(cap * 1.5, vehicle_count * 1.8))

    vehicle_type = random.choice(VEHICLE_TYPES)

    # speed with rare anomalies
    if random.random() < 0.05:
        if random.random() < 0.5:
            speed = random.uniform(5, 15)   # severe congestion
        else:
            speed = random.uniform(85, 110) # very high speed anomaly
    else:
        base_speed = random.uniform(20, 80)
        adjusted_speed = base_speed * (0.8 if rush_factor > 1.0 else 1.2)
        speed = max(5, min(110, round(adjusted_speed, 1)))

    congestion_pct = round(vehicle_count / cap * 100, 2)
    incident = random.choice(TRAFFIC_INCIDENTS) if random.random() < 0.1 else "None"

    event = {
        "Timestamp": now.isoformat(timespec='seconds'),
        "ts": now,  # for easier time filtering/plotting
        "LocationID": location["id"],
        "LocationName": location["name"],
        "Latitude": float(location["lat"]),
        "Longitude": float(location["lon"]),
        "VehicleCount": int(vehicle_count),
        "AverageSpeedKMH": float(round(speed,2)),
        "DominantVehicleType": vehicle_type,
        "WeatherCondition": random.choice(WEATHER_CONDITIONS),
        "TrafficIncident": incident,
        "CongestionPercentage": float(congestion_pct),
        "IsRushHour": rush_factor > 1.0,
        "RushFactor": float(round(rush_factor,2))
    }
    return event

def detect_anomaly(event):
    # Simple rules:
    # - very low speed (<10) or very high speed (>100) -> anomaly
    # - congestion > 120% (over capacity) or > 85% -> anomaly
    # - TrafficIncident != "None" -> anomaly
    alerts = []
    if event["AverageSpeedKMH"] < 10:
        alerts.append("Low speed")
    if event["AverageSpeedKMH"] > 100:
        alerts.append("High speed")
    if event["CongestionPercentage"] > 120:
        alerts.append("Over capacity")
    elif event["CongestionPercentage"] > 85:
        alerts.append("High congestion")
    if event["TrafficIncident"] != "None":
        alerts.append(f"Incident: {event['TrafficIncident']}")
    return alerts

# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="Cairo Traffic Dashboard", layout="wide", initial_sidebar_state="expanded")
st.title("🚦 Cairo Traffic Dashboard")
st.markdown("Real-time simulator and dashboard — Cairo traffic (Map, Analytics, Alerts, Heatmap).")

# Sidebar controls
with st.sidebar:
    st.header("Controls")
    mode = st.selectbox("Mode", options=["Local Simulator (built-in)", "Event Hub (placeholder)"], index=0)
    interval = st.slider("Refresh interval (seconds)", min_value=1, max_value=5, value=SIM_DEFAULT_INTERVAL, step=1)
    run_sim = st.button("Start / Resume")
    stop_sim = st.button("Stop")
    reset_data = st.button("Reset Data")
    st.markdown("---")
    st.markdown("Display Tabs")
    show_overview = st.checkbox("Overview", value=True)
    show_map = st.checkbox("Map", value=True)
    show_analytics = st.checkbox("Analytics", value=True)
    show_alerts = st.checkbox("Alerts", value=True)
    st.markdown("---")
    st.write("Notes:")
    st.write("- Use Local Simulator to run immediately.")
    st.write("- Event Hub mode is a placeholder for future integration.")

# Initialize session state
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()
if "running" not in st.session_state:
    st.session_state.running = False
if "alerts" not in st.session_state:
    st.session_state.alerts = []

# Control actions
if reset_data:
    st.session_state.df = pd.DataFrame()
    st.session_state.alerts = []
    st.success("Data reset.")

if run_sim:
    st.session_state.running = True
if stop_sim:
    st.session_state.running = False

# Main loop behavior (note: Streamlit runs top-to-bottom; we simulate with a loop + rerun)
# We'll generate one new event per run when running, then sleep + rerun through Streamlit rerun mechanism.
# To avoid blocking the UI for long, we generate one event then call st.experimental_rerun after sleep.

def append_event(e):
    df = st.session_state.df
    row = pd.DataFrame([e])
    if df.empty:
        st.session_state.df = row
    else:
        st.session_state.df = pd.concat([df, row], ignore_index=True)
    # trim
    if len(st.session_state.df) > MAX_EVENTS_KEEP:
        st.session_state.df = st.session_state.df.iloc[-MAX_EVENTS_KEEP:].reset_index(drop=True)

# If running mode is simulator, generate events
if mode == "Local Simulator (built-in)" and st.session_state.running:
    # generate one event
    ev = generate_realistic_traffic_data()
    append_event(ev)
    # anomaly detection / alerts
    detected = detect_anomaly(ev)
    if detected:
        st.session_state.alerts.append({
            "timestamp": ev["Timestamp"],
            "location": ev["LocationName"],
            "event": ", ".join(detected),
            "raw": ev
        })
    # wait then rerun to simulate continuous streaming
    time.sleep(interval)
    # rerun the app to update (this creates the "live" feeling)
    st.experimental_rerun()

# Layout: Tabs
tabs = []
if show_overview: tabs.append("Overview")
if show_map: tabs.append("Map")
if show_analytics: tabs.append("Analytics")
if show_alerts: tabs.append("Alerts")

if tabs:
    tab_objs = st.tabs(tabs)
else:
    st.info("Enable at least one tab from the sidebar to see content.")
    tab_objs = []

# ---------- Overview Tab ----------
if show_overview:
    tab = tab_objs[tabs.index("Overview")]
    with tab:
        st.subheader("Overview — Live Snapshot")
        df = st.session_state.df
        if df.empty:
            st.info("No data yet. Click **Start / Resume** to run the local simulator.")
        else:
            latest = df.iloc[-1]
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Events (in memory)", len(df))
            # aggregate last 5 minutes stats
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(minutes=5)
            recent = df[pd.to_datetime(df["ts"]) >= cutoff]
            col2.metric("Events (last 5m)", len(recent))
            col3.metric("Avg Speed (last 5m)", round(recent["AverageSpeedKMH"].mean() if not recent.empty else latest["AverageSpeedKMH"],2))
            col4.metric("Avg Congestion (last 5m %)", round(recent["CongestionPercentage"].mean() if not recent.empty else latest["CongestionPercentage"],2))
            st.markdown("**Latest reading:**")
            st.json({
                "time": latest["Timestamp"],
                "location": latest["LocationName"],
                "vehicles": int(latest["VehicleCount"]),
                "speed": float(latest["AverageSpeedKMH"]),
                "congestion%": float(latest["CongestionPercentage"]),
                "incident": latest["TrafficIncident"]
            })
            # Small charts
            with st.expander("Trends (last 100 events)"):
                tail = df.tail(200)
                if not tail.empty:
                    fig = px.line(tail, x="ts", y=["VehicleCount","CongestionPercentage"], labels={"value":"Count / %","ts":"Time"})
                    st.plotly_chart(fig, use_container_width=True)

# ---------- Map Tab ----------
if show_map:
    tab = tab_objs[tabs.index("Map")]
    with tab:
        st.subheader("Map — Locations & Congestion Heatmap")
        df = st.session_state.df
        if df.empty:
            st.info("No location points yet.")
        else:
            # latest per location: groupby LocationID and take last
            latest_per_loc = df.sort_values("ts").groupby("LocationID").last().reset_index()
            # map markers
            latest_per_loc["color_score"] = latest_per_loc["CongestionPercentage"]
            # pydeck map
            midpoint = (latest_per_loc["Latitude"].mean(), latest_per_loc["Longitude"].mean())
            st.markdown("**Markers (size ~ congestion)**")
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=latest_per_loc,
                get_position='[Longitude, Latitude]',
                get_fill_color='[min(255, int(color_score*2.5)), 50, 150, 200]',
                get_radius='(CongestionPercentage+5) * 20',
                pickable=True,
                auto_highlight=True
            )
            view_state = pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], zoom=11, pitch=30)
            r = pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text":"{LocationName}\nCongestion: {CongestionPercentage}%\nVehicles: {VehicleCount}"})
            st.pydeck_chart(r)

            # heatmap layer (based on all events recent)
            st.markdown("**Congestion Heatmap (recent events)**")
            sample = df.tail(1000) if len(df)>1000 else df
            heat_layer = pdk.Layer(
                "HeatmapLayer",
                data=sample,
                get_position='[Longitude, Latitude]',
                get_weight="CongestionPercentage",
                radiusPixels=60,
            )
            r2 = pdk.Deck(layers=[heat_layer], initial_view_state=view_state)
            st.pydeck_chart(r2)

# ---------- Analytics Tab ----------
if show_analytics:
    tab = tab_objs[tabs.index("Analytics")]
    with tab:
        st.subheader("Analytics")
        df = st.session_state.df
        if df.empty:
            st.info("No data yet for analytics.")
        else:
            # Time window selector
            window = st.radio("Time window", options=["Last 5 minutes", "Last 1 hour", "Last 24 hours", "All"], index=0, horizontal=True)
            now = datetime.now(timezone.utc)
            if window == "Last 5 minutes":
                cutoff = now - timedelta(minutes=5)
            elif window == "Last 1 hour":
                cutoff = now - timedelta(hours=1)
            elif window == "Last 24 hours":
                cutoff = now - timedelta(hours=24)
            else:
                cutoff = df["ts"].min()
            filtered = df[pd.to_datetime(df["ts"]) >= cutoff]

            st.markdown("**Vehicle Count over time**")
            if not filtered.empty:
                fig1 = px.line(filtered, x="ts", y="VehicleCount", title="VehicleCount")
                st.plotly_chart(fig1, use_container_width=True)

            st.markdown("**Average Speed over time**")
            if not filtered.empty:
                fig2 = px.line(filtered, x="ts", y="AverageSpeedKMH", title="AverageSpeedKMH")
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("**Vehicle type distribution**")
            vdist = filtered["DominantVehicleType"].value_counts().reset_index()
            vdist.columns = ["VehicleType","Count"]
            fig3 = px.pie(vdist, names="VehicleType", values="Count", title="Vehicle types")
            st.plotly_chart(fig3, use_container_width=True)

            st.markdown("**Top congested locations (by avg congestion)**")
            toploc = filtered.groupby("LocationName")["CongestionPercentage"].mean().sort_values(ascending=False).reset_index()
            if not toploc.empty:
                st.dataframe(toploc.head(10))

# ---------- Alerts Tab ----------
if show_alerts:
    tab = tab_objs[tabs.index("Alerts")]
    with tab:
        st.subheader("Alerts & Anomalies")
        alerts_df = pd.DataFrame(st.session_state.alerts)
        if alerts_df.empty:
            st.info("No alerts detected yet.")
        else:
            # show latest alerts table
            st.dataframe(alerts_df.sort_values("timestamp", ascending=False).reset_index(drop=True))

            # simple alert stats
            st.markdown("**Alert counts by type (simple text grouping)**")
            # extract basic type from event text (first phrase)
            alerts_df["type_simple"] = alerts_df["event"].apply(lambda s: s.split(",")[0] if isinstance(s,str) else s)
            st.bar_chart(alerts_df["type_simple"].value_counts())

# Footer / Tips
st.markdown("---")
st.caption("Run the Local Simulator and leave the app open — it generates one event per refresh cycle. To integrate with Azure Event Hub/SQL/Blob, contact me and I'll add the connector (consumer) code.")
