from DataParser import DataParser
from DbConnector import DbConnector
from DBWriter import DBWriter


USER="User"
ACTIVITY="Activity"
TRACKPOINT="TrackPoint"

class Task2:
    def __init__(self):
        self.connection = DbConnector(DATABASE="my_db", HOST="localhost", USER="test_user", PASSWORD="test_password")
        self.client = self.connection.client
        self.db = self.connection.db
        self.db_writer = DBWriter(self.connection)
        self.parser = DataParser("sdd-assignment3/dataset")

    def task1(self):
        users_count= self.db[USER].count_documents({})
        print(f"User count: {users_count}")
        activity_count = self.db[ACTIVITY].count_documents({})
        print(f"Activity count: {activity_count}")
        track_point_count=self.db[TRACKPOINT].count_documents({})
        print(f"Trackpoint count: {track_point_count}")

    def task2(self):
        user_list=self.db[USER].find({})
        activity_counts = []
        for user in user_list:
            activity_counts.append(len(user["activities"]))
        
        average_activies_per_user= sum(activity_counts) / len(activity_counts)
        print(f"The average number of activities per user is {average_activies_per_user}")


    def task3(self):
        user_list=self.db[USER].find({})
        users_and_count = [(user["_id"], len(user["activities"])) for user in user_list]
        users_and_count.sort(key=lambda x : x[1], reverse=True)
        twenty_users_with_most_activities = [uac[0] for uac in users_and_count[:20]]
        print(twenty_users_with_most_activities)

    def task4(self):
        user_list=self.db[USER].find({})
        taxi_users = []
        for user in user_list:
            for activity_id in user["activities"]:
                user_activity = self.db[ACTIVITY].find_one({"_id": activity_id})
                if "taxi" ==user_activity["type"]:
                    taxi_users.append(user["_id"])
                    break
        print(taxi_users)


    def task5(self):
        acitivity_list=self.db[ACTIVITY].find({})
        activity_type_dict = {}
        for activity in acitivity_list:
            if activity["type"] in activity_type_dict.keys():
                activity_type_dict[activity["type"]] = activity_type_dict[activity["type"]] + 1
            else:
                activity_type_dict[activity["type"]] = 1
        
        print(activity_type_dict)

    def task6a(self):
        year_activity_dict = {}
        activity_list=self.db[ACTIVITY].find({})
        for activity in activity_list:
            start_year = activity["start_time"].year
            if start_year in year_activity_dict.keys():
                year_activity_dict[start_year] = year_activity_dict[start_year]+1
            else:
                year_activity_dict[start_year] = 1

        max_count = 0
        max_year = None
        for year, count in year_activity_dict.items():
            if count > max_count:
                max_year=year
                max_count=count
        
        print(max_year)


    def task6b(self):
        year_hour_dict={}
        activity_list = self.db[ACTIVITY].find({})
        for activity in activity_list:
            start_year = activity["start_time"].year
            hour_in_activity = activity["end_time"] - activity["start_time"]
            hour_in_activity = hour_in_activity.seconds / 3600
            if start_year in year_hour_dict.keys():
                year_hour_dict[start_year] = year_hour_dict[start_year] + hour_in_activity
            else:
                year_hour_dict[start_year] = hour_in_activity

        max_hour = 0
        max_year = None
        for year, count in year_hour_dict.items():
            if count > max_hour:
                max_year = year
                max_hour = count

        print(max_year)

 
if __name__ == "__main__":
    task = Task2()
    # task.task1()
    # task.task2()
    # task.task3()
    # task.task4()
    # task.task5()
    task.task6a()
    task.task6b()
