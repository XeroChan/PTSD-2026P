import streamlit as st
import json
from confluent_kafka import Consumer

st.set_page_config(page_title="Fraud detector", layout="wide")
st.title("Detektor Anomalii na żywo")

# Konfiguracja konsumenta Kafki
@st.cache_resource
def create_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'streamlit-ui',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['alarms'])
    return consumer

consumer = create_consumer()

# Kontener na alarmy
placeholder = st.empty()

# Pętla czytająca z Kafki (Streamlit odświeża interfejs)
while True:
    msg = consumer.poll(1.0)
    
    if msg is not None and not msg.error():
        alarm = json.loads(msg.value().decode('utf-8'))
        
        with placeholder.container():
            st.error(f"Wykryto anomalię: {alarm['alarm_type']} dla karty {alarm['card_id']}")
            st.json(alarm)