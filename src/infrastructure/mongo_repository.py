from typing import Any, Dict, List, Optional
from pymongo import MongoClient, DESCENDING


# Dostęp do MongoDB: zapis i odczyt alarmów
class MongoRepository:
    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.client = MongoClient(uri)
        self.collection = self.client[db_name][collection_name]
        # Indeks po czasie przyspiesza pobieranie najnowszych alarmów.
        self.collection.create_index([("timestamp", DESCENDING)])

    def save_alarm(self, alarm: Dict[str, Any]) -> None:
        self.collection.insert_one(alarm)

    def count(self) -> int:
        return self.collection.count_documents({})

    # Najnowsze alarmy z bazy (bez pola _id), opcjonalnie filtrowane po typie
    def find_recent(self, limit: int = 200, alarm_type: Optional[str] = None) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {} if alarm_type is None else {"alarm_type": alarm_type}
        cursor = self.collection.find(query, {"_id": 0}).sort("timestamp", DESCENDING).limit(limit)
        return list(cursor)

    # Liczba alarmów wg typu z całej bazy (agregacja po stronie Mongo)
    def count_by_type(self) -> Dict[str, int]:
        pipeline = [{"$group": {"_id": "$alarm_type", "count": {"$sum": 1}}}]
        return {doc["_id"]: doc["count"] for doc in self.collection.aggregate(pipeline)}

    def close(self) -> None:
        self.client.close()
