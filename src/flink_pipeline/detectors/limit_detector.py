import json
from pyflink.datastream.functions import ProcessFunction

# Bezstanowy: alarm gdy kwota transakcji przekracza limit karty
class LimitAnomalyDetector(ProcessFunction):
    def process_element(self, value: str, ctx: 'ProcessFunction.Context'):
        transaction = json.loads(value)
        
        amount = transaction.get('amount', 0.0)
        limit = transaction.get('limit', 0.0)
        
        if amount > limit:
            alarm = {
                "alarm_type": "AMOUNT_LIMIT_EXCEEDED",
                "card_id": transaction.get('card_id'),
                "timestamp": transaction.get('timestamp'),
                "details": {
                    "attempted_amount": amount,
                    "available_limit": limit
                }
            }
            yield json.dumps(alarm)