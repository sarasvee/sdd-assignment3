from bson.objectid import ObjectId        
from datetime import datetime

class DBWriter:
    def __init__(self, db_connector) -> None:
        self.connection = db_connector
        self.client = self.connection.client
        self.db = self.connection.db

    def insert_users_with_activities_trackpoints(self, data, user_collection_name="User", activity_collection_name="Activity", trajectory_collection_name="TrackPoint"):
        user_docs = []
        activity_docs = []
        trajectory_docs = []

        for user in data:
            user_id = user[0]
            user_has_label = len(user[1]) > 0
            activity_ids = []

            for activity in user[1]:
                activity_id = ObjectId()
                activity_ids.append(activity_id)
                trajectory_ids = []

                for trajectory in activity[3]:
                    trajectory_id = ObjectId()
                    trajectory_ids.append(trajectory_id)
                    trajectory_docs.append({
                        "_id": trajectory_id,
                        "lat": trajectory[0],
                        "lon": trajectory[1],
                        "alt": trajectory[2],
                        "date_time": self.parse_date_time(trajectory[3]),
                    })

                activity_docs.append({
                    "_id": activity_id,
                    "start_time": self.parse_date_time(activity[0]),
                    "end_time": self.parse_date_time(activity[1]),
                    "type": activity[2],
                    "trackpoints": trajectory_ids
                })

            user_docs.append( {
                    "_id": user_id,
                    "has_labels": user_has_label,
                    "activities": activity_ids
                })

        collection = self.db[trajectory_collection_name]
        collection.insert_many(trajectory_docs)
        
        collection = self.db[activity_collection_name]
        collection.insert_many(activity_docs)

        collection = self.db[user_collection_name]
        collection.insert_many(user_docs)

    @staticmethod
    def parse_date_time(date_time: str) -> datetime:
        return datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")

