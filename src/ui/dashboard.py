import streamlit as st
import pandas as pd
import json
import time
from confluent_kafka import Consumer
from src.config.settings import KAFKA_BROKER, TOPIC_ALARMS

st.set_page_config(page_title="Fraud Detection", page_icon="🚨", layout="wide")
st.title("🚨 Live Fraud Detection Dashboard")
st.markdown("Real-time anomaly monitoring system. Data source: **Apache Flink**.")

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

try:
    consumer = get_kafka_consumer()
except Exception as e:
    st.error(f"Connection error: {e}")
    st.stop()

placeholder = st.empty()
force_update = True 

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
                has_new_data = True
        except Exception as e:
            print(f"Message parsing error: {e}")
    
    if has_new_data or force_update:
        with placeholder.container():
            st.metric(label="Detected Anomalies (session)", value=len(st.session_state.alarms_data))
            
            if st.session_state.alarms_data:
                df = pd.json_normalize(st.session_state.alarms_data)
                
                st.subheader("💰 Fraud: Limit Exceeded")
                df_limit = df[df['alarm_type'] == 'AMOUNT_LIMIT_EXCEEDED'].dropna(axis=1, how='all')
                
                if not df_limit.empty:
                    st.dataframe(df_limit.fillna(""), use_container_width=True, height=200)
                else:
                    st.info("No alarms of this type in the current session.")
                    
                st.markdown("---")
                
                st.subheader("🌍 Fraud: Impossible Travel")
                df_location = df[df['alarm_type'] == 'IMPOSSIBLE_TRAVEL'].dropna(axis=1, how='all')
                
                if not df_location.empty:
                    st.dataframe(df_location.fillna(""), use_container_width=True, height=200)
                else:
                    st.info("No alarms of this type in the current session.")
                
                st.markdown("---")
                
                col_chart_left, col_chart_right = st.columns(2)
                
                with col_chart_left:
                    st.subheader("📊 Fraud Distribution")
                    chart_data = df['alarm_type'].value_counts().reset_index()
                    chart_data.columns = ['Alarm Type', 'Count']
                    st.bar_chart(chart_data, x='Alarm Type', y='Count', use_container_width=True)
                
                with col_chart_right:
                    st.subheader("📈 Trend over time")
                    df['time'] = pd.to_datetime(df['timestamp'])
                    df_sorted = df.sort_values('time')
                    df_sorted['Cumulative Count'] = range(1, len(df_sorted) + 1)
                    
                    st.line_chart(df_sorted, x='time', y='Cumulative Count', use_container_width=True)
                    
            else:
                st.info("Listening in real-time... Waiting for frauds. 🕵️‍♂️")
        
        force_update = False 
            
    time.sleep(0.1)