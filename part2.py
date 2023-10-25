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

    def task2(self):
        user_list=self.db.User.find({})
        activity_counts = []
        for user in user_list:
            activity_counts.append(len(user["activities"]))
        
        average_activies_per_user= sum(activity_counts) / len(activity_counts)
        print(f"The average number of activities per user is {average_activies_per_user}")


    def task3(self):
        user_list=self.db.User.find({})
        users_and_count = [(user["_id"], len(user["activities"])) for user in user_list]
        users_and_count.sort(key=lambda x : x[1], reverse=True)
        twenty_users_with_most_activities = [uac[0] for uac in users_and_count[:20]]
        print(twenty_users_with_most_activities)

    def task4(self):
        user_list=self.db.User.find({})
        taxi_users = []
        for user in user_list:
            for activity_id in user["activities"]:
                user_activity = self.db.Activity.find_one({"_id": activity_id})
                if "taxi" ==user_activity["type"]:
                    taxi_users.append(user["_id"])
                    break
        print(taxi_users)



if __name__ == "__main__":
    task = Task2()
    # task.task1()
    # task.task2()
    # task.task3()
    task.task4()
