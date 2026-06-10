import streamlit as st
import pandas as pd
import json
import time
from confluent_kafka import Consumer
from src.config.settings import (
    KAFKA_BROKER, TOPIC_ALARMS, TOPIC_RAW,
    MONGO_URI, MONGO_DB, MONGO_COLLECTION_ALARMS,
)
from src.ui.aggregations import Aggregates, build_stream
from src.infrastructure.mongo_repository import MongoRepository

st.set_page_config(page_title="Fraud Detection", page_icon="🚨", layout="wide")
st.title("🚨 Live Fraud Detection Dashboard")
st.markdown("Real-time anomaly monitoring system. Data source: **Apache Flink**, aggregation: **streamz**.")

if 'alarms_data' not in st.session_state:
    st.session_state.alarms_data = []
if 'raw_stats' not in st.session_state:
    st.session_state.raw_stats = {
        'unique_cards': set(),
        'normal_count': 0,
        'anomaly_count': 0,
        'total_amount': 0.0,
        'count': 0
    }
if 'raw_data' not in st.session_state:
    st.session_state.raw_data = []

@st.cache_resource
def get_kafka_consumer():
    max_retries = 10
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'streamlit-dashboard-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    for i in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka ({KAFKA_BROKER})... attempt {i+1}")
            consumer = Consumer(conf)
            consumer.subscribe([TOPIC_ALARMS, TOPIC_RAW])
            return consumer
        except Exception as e:
            print(f"Failed to connect (yet): {e}")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka after 10 attempts!")


@st.cache_resource
def get_pipeline():
    """Buduje potok streamz raz; aktualizuje agregaty w miarę napływu alarmów."""
    agg = Aggregates()
    source = build_stream(agg)
    return source, agg


@st.cache_resource
def get_mongo():
    return MongoRepository(MONGO_URI, MONGO_DB, MONGO_COLLECTION_ALARMS)


try:
    consumer = get_kafka_consumer()
except Exception as e:
    st.error(f"Connection error: {e}")
    st.stop()

source, agg = get_pipeline()
mongo = get_mongo()

tab_alarms, tab_raw = st.tabs(["🚨 Alarms", "📊 Raw Transactions"])

placeholder_alarms = tab_alarms.empty()
placeholder_raw = tab_raw.empty()
force_update = True

DB_REFRESH_SECONDS = 3
last_db_refresh = 0.0
db_history = []
db_counts = {}


def render_alarm_table(df, alarm_type, header, empty_msg):
    st.subheader(header)
    if df is not None and alarm_type in df['alarm_type'].values:
        subset = df[df['alarm_type'] == alarm_type].dropna(axis=1, how='all')
        st.dataframe(subset.fillna("").astype(str), width='stretch', height=200)
    else:
        st.info(empty_msg)


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
                    st.session_state.alarms_data.insert(0, value)
                    st.session_state.alarms_data = st.session_state.alarms_data[:100]
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
            db_refreshed = True
        except Exception as e:
            print(f"Mongo read error: {e}")
        last_db_refresh = time.time()

    if has_new_data or force_update or db_refreshed:
        with placeholder_alarms.container():
            # --- Metryki z potoku streamz ---
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Wykryte anomalie (łącznie)", int(sum(agg.by_type.values())))
            col_b.metric("Kwota zagrożona", f"{agg.money_at_risk:,.2f}")
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
                col_left.dataframe(type_df, width='stretch', hide_index=True)
                col_right.bar_chart(type_df, x='Alarm Type', y='Count', width='stretch')
            else:
                st.info("Brak alarmów w tej sesji.")

            st.markdown("---")

            # --- Tabele szczegółowe (ostatnie rekordy) ---
            if st.session_state.alarms_data:
                df = pd.json_normalize(st.session_state.alarms_data)
                render_alarm_table(df, 'AMOUNT_LIMIT_EXCEEDED',
                                   "💰 Fraud: Limit Exceeded",
                                   "No alarms of this type in the current session.")
                st.markdown("---")
                render_alarm_table(df, 'AMOUNT_ZSCORE_ANOMALY',
                                   "📈 Fraud: Statistical Amount Anomaly (z-score)",
                                   "No alarms of this type in the current session.")
                st.markdown("---")
                render_alarm_table(df, 'IMPOSSIBLE_TRAVEL',
                                   "🌍 Fraud: Impossible Travel",
                                   "No alarms of this type in the current session.")
                st.markdown("---")
                render_alarm_table(df, 'UNUSUAL_LOCATION',
                                   "📍 Fraud: Unusual Location",
                                   "No alarms of this type in the current session.")
                st.markdown("---")
                render_alarm_table(df, 'HIGH_FREQUENCY_TRANSACTIONS',
                                   "⏱️ Fraud: High Frequency Transactions",
                                   "No alarms of this type in the current session.")
            else:
                st.info("Listening in real-time... Waiting for frauds. 🕵️‍♂️")

            # --- Pełna historia z MongoDB (wszystkie sesje, nie tylko bieżąca) ---
            st.markdown("---")
            st.subheader("🗄️ Pełna historia alarmów (MongoDB)")
            st.metric("Alarmy zapisane w bazie (wszystkie sesje)", sum(db_counts.values()))
            if db_counts:
                db_counts_df = pd.DataFrame(
                    sorted(db_counts.items()), columns=['Alarm Type', 'Count w bazie']
                )
                st.dataframe(db_counts_df, width='stretch', hide_index=True)
            if db_history:
                st.caption(f"Ostatnie {len(db_history)} alarmów z bazy:")
                hist_df = pd.json_normalize(db_history)
                st.dataframe(hist_df.fillna("").astype(str), width='stretch', height=300)
            else:
                st.info("Baza pusta lub niedostępna.")

    if has_new_raw_data or force_update:
        with placeholder_raw.container():
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
            
            st.metric("Average Transaction Amount", f"{avg_amount:.2f}")
            
            st.markdown("---")
            st.subheader("Latest Raw Transactions")
            if st.session_state.raw_data:
                raw_df = pd.json_normalize(st.session_state.raw_data)
                st.dataframe(raw_df.astype(str), width='stretch', height=400)
            else:
                st.info("Waiting for raw transactions...")

    force_update = False
    time.sleep(0.1)
