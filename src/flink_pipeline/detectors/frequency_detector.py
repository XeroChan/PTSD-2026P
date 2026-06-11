import json
from datetime import datetime
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from src.domain.alarm import Alarm

TIME_WINDOW_SECONDS = 10.0
MAX_TRANSACTIONS = 4

# Alarm gdy karta wykona za dużo transakcji w krótkim oknie czasowym
class FrequencyAnomalyDetector(KeyedProcessFunction):
    def __init__(self):
        self.timestamps_state = None

    def open(self, runtime_context: RuntimeContext):
        self.timestamps_state = runtime_context.get_state(
            ValueStateDescriptor("timestamps_state", Types.STRING()))

    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context'):
        current_tx = json.loads(value)
        card_id = current_tx.get('card_id')
        current_time_str = current_tx['timestamp']
        
        try:
            current_time = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))
        except Exception:
            current_time = datetime.utcnow()

        timestamps_str = self.timestamps_state.value()
        timestamps = json.loads(timestamps_str) if timestamps_str else []

        valid_timestamps = []
        for ts_str in timestamps:
            try:
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                if (current_time - ts).total_seconds() <= TIME_WINDOW_SECONDS:
                    valid_timestamps.append(ts_str)
            except:
                pass

        valid_timestamps.append(current_time_str)
        
        if len(valid_timestamps) > MAX_TRANSACTIONS:
            alarm = Alarm(
                alarm_type='HIGH_FREQUENCY_TRANSACTIONS',
                card_id=card_id,
                timestamp_iso=current_time_str,
                details={
                    'transaction_count': len(valid_timestamps),
                    'time_window_seconds': TIME_WINDOW_SECONDS
                },
            )
            yield json.dumps(alarm.to_dict())
            valid_timestamps = []

        self.timestamps_state.update(json.dumps(valid_timestamps))
