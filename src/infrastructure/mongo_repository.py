from typing import Any, Dict
from pymongo import MongoClient


class MongoRepository:
    """Prosta warstwa dostępu do MongoDB dla zapisu alarmów."""

    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.client = MongoClient(uri)
        self.collection = self.client[db_name][collection_name]

    def save_alarm(self, alarm: Dict[str, Any]) -> None:
        self.collection.insert_one(alarm)

    def count(self) -> int:
        return self.collection.count_documents({})

    def close(self) -> None:
        self.client.close()
