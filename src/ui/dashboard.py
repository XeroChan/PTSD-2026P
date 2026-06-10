import streamlit as st
import pandas as pd
import json
import time
from collections import Counter
from confluent_kafka import Consumer
from src.config.settings import (
    KAFKA_BROKER, TOPIC_ALARMS, TOPIC_RAW,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION_ALARMS,
)
from src.ui.aggregations import Aggregates, build_stream
from src.infrastructure.mongo_repository import MongoRepository

st.set_page_config(page_title="Fraud Detection Dashboard", page_icon="🚨", layout="wide")

# --- Custom CSS for a more modern look ---
st.markdown("""
<style>
    .reportview-container .main .block-container{
        padding-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚨 Live Fraud Detection Dashboard")
st.markdown("Real-time anomaly monitoring system. Data source: **Apache Flink**, aggregation: **streamz**.")

if 'alarms_by_type' not in st.session_state:
    st.session_state.alarms_by_type = {}


def alarm_amount_at_risk(alarm: dict) -> float:
    details = alarm.get('details', {})
    value = details.get('attempted_amount')
    if value is None:
        value = details.get('amount')
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def alarm_abs_zscore(alarm: dict) -> float:
    if alarm.get('alarm_type') != 'AMOUNT_ZSCORE_ANOMALY':
        return 0.0
    details = alarm.get('details', {})
    try:
        return abs(float(details.get('z_score', 0.0)))
    except (TypeError, ValueError):
        return 0.0


def sync_dashboard_from_history(history, counts) -> None:
    grouped = {}
    for alarm in history:
        alarm_type = alarm.get('alarm_type', 'UNKNOWN')
        grouped.setdefault(alarm_type, []).append(alarm)

    st.session_state.alarms_by_type = {
        alarm_type: alarms[:20]
        for alarm_type, alarms in grouped.items()
    }

    agg.set_by_type(Counter(counts))
    agg.set_money(sum(alarm_amount_at_risk(alarm) for alarm in history))
    agg.set_max_zscore(max((alarm_abs_zscore(alarm) for alarm in history), default=0.0))

def render_alarm_table(alarms_list, empty_msg):
    if alarms_list:
        df = pd.json_normalize(alarms_list)
        st.dataframe(df.fillna("").astype(str), use_container_width=True, height=200)
    else:
        st.info(empty_msg)

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'dashboard_ui',
    'auto.offset.reset': 'latest'
})
consumer.subscribe([TOPIC_ALARMS, TOPIC_RAW])

if 'raw_data' not in st.session_state:
    st.session_state.raw_data = []

if 'raw_stats' not in st.session_state:
    st.session_state.raw_stats = {
        'unique_cards': set(),
        'count': 0,
        'total_amount': 0.0,
        'anomaly_count': 0,
        'normal_count': 0
    }

agg = Aggregates()
source = build_stream(agg)
mongo = MongoRepository(MONGO_URI, MONGO_DB, MONGO_COLLECTION_ALARMS)

last_db_refresh = 0
DB_REFRESH_SECONDS = 5
db_history = []
db_counts = {}
force_update = True

try:
    db_history = mongo.find_recent(limit=200)
    db_counts = mongo.count_by_type()
    sync_dashboard_from_history(db_history, db_counts)
except Exception as e:
    print(f"Mongo bootstrap error: {e}")

# --- Navigation via Tabs ---
tab_overview, tab_alerts, tab_raw, tab_db = st.tabs([
    "📊 Overview", 
    "🚨 Alert Details", 
    "📡 Raw Transactions", 
    "🗄️ Database History"
])

with tab_overview:
    ph_overview = st.empty()
with tab_alerts:
    ph_alerts = st.empty()
with tab_raw:
    ph_raw = st.empty()
with tab_db:
    ph_db = st.empty()

while True:
    msg = consumer.poll(timeout=0.5)
    has_new_data = False
    has_new_raw_data = False

    if msg is None:
        pass
    elif msg.error():
        print(f"Consumer error: {msg.error()}")
    else:
        try:
            payload = msg.value()
            if payload is not None:
                value = json.loads(payload.decode('utf-8'))
                topic = msg.topic()
                
                if topic == TOPIC_ALARMS:
                    alarm_type = value.get('alarm_type', 'UNKNOWN')
                    if alarm_type not in st.session_state.alarms_by_type:
                        st.session_state.alarms_by_type[alarm_type] = []
                    
                    st.session_state.alarms_by_type[alarm_type].insert(0, value)
                    st.session_state.alarms_by_type[alarm_type] = st.session_state.alarms_by_type[alarm_type][:20]
                    
                    # potok streamz: przeliczenie metryk bez petli agregujacej
                    source.emit(value)
                    has_new_data = True
                elif topic == TOPIC_RAW:
                    st.session_state.raw_data.insert(0, value)
                    st.session_state.raw_data = st.session_state.raw_data[:100]
                    
                    st.session_state.raw_stats['unique_cards'].add(value.get('card_id'))
                    st.session_state.raw_stats['count'] += 1
                    st.session_state.raw_stats['total_amount'] += value.get('amount', 0.0)
                    if value.get('is_anomaly', False):
                        st.session_state.raw_stats['anomaly_count'] += 1
                    else:
                        st.session_state.raw_stats['normal_count'] += 1
                    
                    has_new_raw_data = True

        except Exception as e:
            print(f"Message parsing error: {e}")

    # Odświeżanie historii z bazy - co kilka sekund, nie co iterację pętli.
    db_refreshed = False
    if time.time() - last_db_refresh > DB_REFRESH_SECONDS:
        try:
            db_history = mongo.find_recent(limit=200)
            db_counts = mongo.count_by_type()
            sync_dashboard_from_history(db_history, db_counts)
            db_refreshed = True
        except Exception as e:
            print(f"Mongo read error: {e}")
        last_db_refresh = time.time()

    if has_new_data or force_update or db_refreshed:
        with ph_overview.container():
            # --- Metryki z potoku streamz ---
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Wykryte anomalie (łącznie)", int(sum(agg.by_type.values())))
            col_b.metric("Kwota zagrożona", f"${agg.money_at_risk:,.2f}")
            col_c.metric("Maks. z-score", f"{agg.max_zscore:.2f}")

            st.markdown("---")

            # --- Licznik alarmów wg typu (z potoku) ---
            st.subheader("📊 Alarmy wg typu")
            if agg.by_type:
                type_df = pd.DataFrame(
                    sorted(agg.by_type.items()),
                    columns=['Alarm Type', 'Count']
                )
                col_left, col_right = st.columns([1, 2])
                col_left.dataframe(type_df, use_container_width=True, hide_index=True)
                col_right.bar_chart(type_df, x='Alarm Type', y='Count', use_container_width=True)
            else:
                st.info("Brak alarmów w tej sesji.")

        with ph_alerts.container():
            # --- Tabele szczegółowe (ostatnie rekordy) ---
            if st.session_state.alarms_by_type:
                with st.expander("💰 Fraud: Limit Exceeded", expanded=True):
                    render_alarm_table(st.session_state.alarms_by_type.get('AMOUNT_LIMIT_EXCEEDED', []),
                                       "No alarms of this type in the current session.")
                
                with st.expander("📈 Fraud: Statistical Amount Anomaly (z-score)", expanded=True):
                    render_alarm_table(st.session_state.alarms_by_type.get('AMOUNT_ZSCORE_ANOMALY', []),
                                       "No alarms of this type in the current session.")
                
                with st.expander("🌍 Fraud: Impossible Travel"):
                    render_alarm_table(st.session_state.alarms_by_type.get('IMPOSSIBLE_TRAVEL', []),
                                       "No alarms of this type in the current session.")
                
                with st.expander("📍 Fraud: Unusual Location"):
                    render_alarm_table(st.session_state.alarms_by_type.get('UNUSUAL_LOCATION', []),
                                       "No alarms of this type in the current session.")
                
                with st.expander("⏱️ Fraud: High Frequency Transactions"):
                    render_alarm_table(st.session_state.alarms_by_type.get('HIGH_FREQUENCY_TRANSACTIONS', []),
                                       "No alarms of this type in the current session.")
            else:
                st.info("Listening in real-time... Waiting for frauds. 🕵️‍♂️")

        with ph_db.container():
            # --- Pełna historia z MongoDB (wszystkie sesje, nie tylko bieżąca) ---
            st.subheader("🗄️ Pełna historia alarmów (MongoDB)")
            
            c1, c2 = st.columns([1, 3])
            with c1:
                st.metric("Alarmy zapisane w bazie", sum(db_counts.values()))
                if db_counts:
                    db_counts_df = pd.DataFrame(
                        sorted(db_counts.items()), columns=['Alarm Type', 'Count w bazie']
                    )
                    st.dataframe(db_counts_df, use_container_width=True, hide_index=True)
            
            with c2:
                if db_history:
                    st.caption(f"Ostatnie {len(db_history)} alarmów z bazy:")
                    hist_df = pd.json_normalize(db_history)
                    st.dataframe(hist_df.fillna("").astype(str), use_container_width=True, height=400)
                else:
                    st.info("Baza pusta lub niedostępna.")

    if has_new_raw_data or force_update:
        with ph_raw.container():
            # --- Raw Data Monitor ---
            st.subheader("📡 Raw Transactions Monitor")
            st.markdown("Monitor the generator's health and statistics.")
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Processed Trx", st.session_state.raw_stats['count'])
            c2.metric("Unique Cards", len(st.session_state.raw_stats['unique_cards']))
            c3.metric("Normal Trx", st.session_state.raw_stats['normal_count'])
            c4.metric("Anomalies Generated", st.session_state.raw_stats['anomaly_count'])
            
            avg_amount = 0
            if st.session_state.raw_stats['count'] > 0:
                avg_amount = st.session_state.raw_stats['total_amount'] / st.session_state.raw_stats['count']
            
            st.metric("Average Transaction Amount", f"${avg_amount:,.2f}")
            
            st.markdown("---")
            st.subheader("Latest Raw Transactions")
            if st.session_state.raw_data:
                raw_df = pd.json_normalize(st.session_state.raw_data)
                st.dataframe(raw_df.astype(str), use_container_width=True, height=400)
            else:
                st.info("Waiting for raw transactions...")

    force_update = False
    time.sleep(0.1)
