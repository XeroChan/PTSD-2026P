import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Konfiguracja producenta Kafki (łączymy się z portem wystawionym na zewnątrz Dockera)
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = 'raw_transactions'

# Generowanie bazy 10 000 kart
print("Generowanie bazy kart...")
cards = [{'card_id': f'CARD_{i}', 'user_id': fake.uuid4(), 'limit': random.uniform(1000, 20000)} for i in range(10000)]

def generate_transaction(card, is_anomaly=False):
    # Podstawowe dane
    tx = {
        'card_id': card['card_id'],
        'user_id': card['user_id'],
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'amount': round(random.uniform(5, 500), 2),
        'available_limit': card['limit'],
        'lat': float(fake.latitude()),
        'lon': float(fake.longitude())
    }
    
    # Wstrzyknięcie anomalii kwotowej (przekroczenie limitu / ogromna kwota)
    if is_anomaly:
        tx['amount'] = round(card['limit'] * random.uniform(1.1, 2.0), 2)
        
    return tx

def delivery_report(err, msg):
    if err is not None:
        print(f"Błąd dostarczenia wiadomości: {err}")

# Pętla główna symulatora
print("Rozpoczęcie wysyłania transakcji...")
while True:
    card = random.choice(cards)
    
    # Z prawdopodobieństwem 1% generujemy anomalię
    is_anomaly = random.random() < 0.01 
    tx_data = generate_transaction(card, is_anomaly)
    
    # Wysyłanie do Kafki
    producer.produce(topic, key=tx_data['card_id'], value=json.dumps(tx_data), callback=delivery_report)
    producer.poll(0) # Odświeżenie bufora
    
    time.sleep(0.1) # Generujemy ok. 10 transakcji na sekundę dla testów