from typing import Any, Dict, List, Optional
from pymongo import MongoClient, DESCENDING


class MongoRepository:
    """Warstwa dostępu do MongoDB: zapis i odczyt alarmów."""

    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.client = MongoClient(uri)
        self.collection = self.client[db_name][collection_name]
        # Indeks po czasie przyspiesza pobieranie najnowszych alarmów.
        self.collection.create_index([("timestamp", DESCENDING)])

    def save_alarm(self, alarm: Dict[str, Any]) -> None:
        self.collection.insert_one(alarm)

    def count(self) -> int:
        return self.collection.count_documents({})

    def find_recent(self, limit: int = 200, alarm_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Najnowsze alarmy z bazy (bez pola _id), opcjonalnie filtrowane po typie."""
        query: Dict[str, Any] = {} if alarm_type is None else {"alarm_type": alarm_type}
        cursor = self.collection.find(query, {"_id": 0}).sort("timestamp", DESCENDING).limit(limit)
        return list(cursor)

    def count_by_type(self) -> Dict[str, int]:
        """Liczba alarmów wg typu z całej bazy (wszystkie sesje)."""
        pipeline = [{"$group": {"_id": "$alarm_type", "count": {"$sum": 1}}}]
        return {doc["_id"]: doc["count"] for doc in self.collection.aggregate(pipeline)}

    def close(self) -> None:
        self.client.close()
