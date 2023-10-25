from DataParser import DataParser
from DbConnector import DbConnector
from DBWriter import DBWriter

class Task2:
    def __init__(self):
        self.connection = DbConnector(DATABASE="my_db", HOST="localhost", USER="test_user", PASSWORD="test_password")
        self.client = self.connection.client
        self.db = self.connection.db
        self.db_writer = DBWriter(self.connection)
        self.parser = DataParser("sdd-assignment3/dataset")

    def task1(self):
        users_count= self.db.User.count_documents({})
        print(f"User count: {users_count}")
        activity_count = self.db.Activity.count_documents({})
        print(f"Activity count: {activity_count}")
        track_point_count=self.db.TrackPoint.count_documents({})
        print(f"Trackpoint count: {track_point_count}")

if __name__ == "__main__":
    task = Task2()
    task.task1()
