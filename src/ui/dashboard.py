import streamlit as st
import pandas as pd
import json
import time
from confluent_kafka import Consumer
from src.config.settings import KAFKA_BROKER, TOPIC_ALARMS
from src.ui.aggregations import Aggregates, build_stream

st.set_page_config(page_title="Fraud Detection", page_icon="🚨", layout="wide")
st.title("🚨 Live Fraud Detection Dashboard")
st.markdown("Real-time anomaly monitoring system. Data source: **Apache Flink**, aggregation: **streamz**.")

if 'alarms_data' not in st.session_state:
    st.session_state.alarms_data = []


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
            consumer.subscribe([TOPIC_ALARMS])
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


try:
    consumer = get_kafka_consumer()
except Exception as e:
    st.error(f"Connection error: {e}")
    st.stop()

source, agg = get_pipeline()

placeholder = st.empty()
force_update = True


def render_alarm_table(df, alarm_type, header, empty_msg):
    st.subheader(header)
    if df is not None and alarm_type in df['alarm_type'].values:
        subset = df[df['alarm_type'] == alarm_type].dropna(axis=1, how='all')
        st.dataframe(subset.fillna(""), use_container_width=True, height=200)
    else:
        st.info(empty_msg)


while True:
    msg = consumer.poll(timeout=0.5)
    has_new_data = False

    if msg is None:
        pass
    elif msg.error():
        print(f"Consumer error: {msg.error()}")
    else:
        try:
            payload = msg.value()
            if payload is not None:
                value = json.loads(payload.decode('utf-8'))
                st.session_state.alarms_data.insert(0, value)
                st.session_state.alarms_data = st.session_state.alarms_data[:100]
                # potok streamz: przeliczenie metryk bez petli agregujacej
                source.emit(value)
                has_new_data = True
        except Exception as e:
            print(f"Message parsing error: {e}")

    if has_new_data or force_update:
        with placeholder.container():
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
                col_left.dataframe(type_df, use_container_width=True, hide_index=True)
                col_right.bar_chart(type_df, x='Alarm Type', y='Count', use_container_width=True)
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
            else:
                st.info("Listening in real-time... Waiting for frauds. 🕵️‍♂️")

        force_update = False

    time.sleep(0.1)
